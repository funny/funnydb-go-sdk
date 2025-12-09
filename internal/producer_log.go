package internal

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type LogProducerConfig struct {
	Directory string
	FileSize  int64
}

type LogProducer struct {
	status     int32
	config     *LogProducerConfig
	dateFormat string
	fileSize   int64
	wg         sync.WaitGroup
	ch         chan *LogProducerRequest
}

type LogProducerRequest struct {
	data []byte
	done chan struct{}
	err  error
}

func NewLogProducer(config LogProducerConfig) (Producer, error) {
	p := LogProducer{
		status:     running,
		config:     &config,
		ch:         make(chan *LogProducerRequest),
		dateFormat: time.DateOnly,
		fileSize:   config.FileSize * 1024 * 1024,
		wg:         sync.WaitGroup{},
	}
	return &p, p.init()
}

func (p *LogProducer) Add(ctx context.Context, data map[string]interface{}) error {
	var err error = nil

	if atomic.LoadInt32(&p.status) == stop {
		err = ErrProducerClosed
	} else {
		jsonData, jsonErr := marshalToBytes(data)
		if jsonErr != nil {
			err = jsonErr
		} else {
			req := &LogProducerRequest{
				data: jsonData,
				err:  nil,
				done: make(chan struct{}),
			}

			p.ch <- req
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-req.done:
				return req.err
			}
		}
	}

	return err
}

func (p *LogProducer) Close(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&p.status, running, stop) {
		close(p.ch)
		// 等待写入协程结束
		p.wg.Wait()
		return nil
	} else {
		return ErrProducerClosed
	}
}

func (p *LogProducer) init() error {
	p.wg.Add(1)

	go p.runWork()

	DefaultLogger.Infof("ModePersistOnly staring, log path: %s", p.config.Directory)

	return nil
}

func (p *LogProducer) runWork() {
	defer func() {
		atomic.StoreInt32(&p.status, stop)
		p.wg.Done()
	}()

	var totalSize int64 = 0
	currentFile, writeDirectory, err := p.createLogFile()
	if err != nil {
		DefaultLogger.Errorf("Create log file error: %s", err)
		return
	}

	for req := range p.ch {
		writtenSize := int64(len(req.data)) + 1 // +1 for '\n'

		expectWriteDirectory := generateLogDirectory(p.config.Directory, time.Now())
		if checkNeedLogRotate(writeDirectory, expectWriteDirectory, totalSize+writtenSize, p.fileSize) {
			currentFile, writeDirectory, err = p.rotateLogFile(currentFile)
			if err != nil {
				DefaultLogger.Errorf("Rotate log file error: %s", err)
				return
			}
			totalSize = 0
		}

		_, writeErr := currentFile.Write(append(req.data, '\n'))
		req.err = writeErr
		close(req.done)
		totalSize += writtenSize
	}

	if err := closeLogFile(currentFile); err != nil {
		DefaultLogger.Errorf("Close log file %s error: %s", currentFile.Name(), err)
	}
}

func (p *LogProducer) createLogFile() (*os.File, string, error) {
	for i := 0; ; i++ {
		dir, _, logPath := p.getCurrentTimeLogFileInfo(i)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, "", err
		}

		// create file atomically by using O_CREATE and O_EXCL flags.
		file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_EXCL, 0664)
		if err != nil {
			if os.IsExist(err) {
				continue
			}
			return nil, "", err
		}
		DefaultLogger.Infof("Create log file: %s", logPath)
		return file, dir, nil
	}
}

func (p *LogProducer) rotateLogFile(currentFile *os.File) (*os.File, string, error) {
	if err := closeLogFile(currentFile); err != nil {
		return nil, "", err
	}
	currentFile, writeDirectory, err := p.createLogFile()
	if err != nil {
		return nil, "", err
	}

	return currentFile, writeDirectory, err
}

func (p *LogProducer) getCurrentTimeLogFileInfo(index int) (string, string, string) {
	return GetLogFileInfo(time.Now(), p.config.Directory, p.dateFormat, index)
}

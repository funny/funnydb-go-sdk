package internal

import (
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type LogProducerConfig struct {
	Directory string // directory of log file
	FileSize  int64  // max size of single log file (MByte)
}

type LogProducer struct {
	status       int32
	config       *LogProducerConfig
	dateFormat   string
	fileSize     int64
	logFileIndex int
	m            sync.Mutex
	wg           sync.WaitGroup
	ch           chan *LogProducerRequest
}

type LogProducerRequest struct {
	data []byte
	done chan struct{}
	err  error
}

func NewLogProducer(config LogProducerConfig) (Producer, error) {
	p := LogProducer{
		status:       running,
		config:       &config,
		ch:           make(chan *LogProducerRequest),
		dateFormat:   time.DateOnly,
		fileSize:     config.FileSize * 1024 * 1024,
		logFileIndex: 0,
		m:            sync.Mutex{},
		wg:           sync.WaitGroup{},
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

	log.Println("ModePersistOnly staring, log path: " + p.config.Directory)

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
		log.Printf("Create log file error: %s\n", err)
		return
	}

	for {
		select {
		case req, ok := <-p.ch:
			if !ok {
				if err := closeLogFile(currentFile); err != nil {
					log.Printf("Close log file error: %s\n", err)
				}
				return
			}

			expectWriteDirectory := generateLogDirectory(p.config.Directory, time.Now())
			if checkNeedLogRotate(writeDirectory, expectWriteDirectory, totalSize, p.fileSize) {
				currentFile, writeDirectory, err = p.rotateLogFile(currentFile)
				if err != nil {
					log.Printf("Rotate log file error: %s\n", err)
					return
				}
				totalSize = 0
			}

			writeLen, writeErr := writeToFile(currentFile, req.data)
			req.err = writeErr
			totalSize = totalSize + int64(writeLen)
			close(req.done)
		}
	}
}

func (p *LogProducer) createLogFile() (*os.File, string, error) {
	p.m.Lock()
	defer p.m.Unlock()

	logDirectory, _, _ := p.getCurrentTimeLogFileInfo()

	_, err := os.Stat(logDirectory)
	if err != nil && os.IsNotExist(err) {
		e := os.MkdirAll(logDirectory, os.ModePerm)
		if e != nil {
			return nil, "", e
		}
	}

	p.logFileIndex = calculateLogFileIndex(logDirectory, p.dateFormat, time.Now())
	_, _, logPath := p.getCurrentTimeLogFileInfo()

	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0664)

	return file, logDirectory, err
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

func (p *LogProducer) getCurrentTimeLogFileInfo() (string, string, string) {
	return GetLogFileInfo(time.Now(), p.config.Directory, p.dateFormat, p.logFileIndex)
}

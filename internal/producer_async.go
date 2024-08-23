package internal

import (
	"context"
	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
	"golang.org/x/sync/errgroup"
	"sync/atomic"
	"time"
)

type AsyncProducerConfig struct {
	Directory        string
	IngestEndpoint   string
	AccessKey        string
	AccessSecret     string
	MaxBufferRecords int
	SendInterval     time.Duration
	SendTimeout      time.Duration
	BatchSize        int64
}

type AsyncProducer struct {
	status       int32
	config       *AsyncProducerConfig
	q            Queue
	eg           *errgroup.Group
	egCtx        context.Context
	closeCh      chan interface{}
	ingestClient *client.Client
}

func NewAsyncProducer(config AsyncProducerConfig) (Producer, error) {
	ingestClient, err := client.NewClient(client.Config{
		Endpoint:        config.IngestEndpoint,
		AccessKeyID:     config.AccessKey,
		AccessKeySecret: config.AccessSecret,
	})
	if err != nil {
		return nil, err
	}
	dq := New(
		"funnydb",
		config.Directory,
		128*1024*1024, // 128MB
		1,
		10*1024*1024, // 10MB
		500,
		2*time.Second,
	)
	eg, ctx := errgroup.WithContext(context.Background())

	p := AsyncProducer{
		status:       running,
		config:       &config,
		q:            dq,
		eg:           eg,
		egCtx:        ctx,
		closeCh:      make(chan interface{}),
		ingestClient: ingestClient,
	}
	return &p, p.init()
}

func (p *AsyncProducer) Add(ctx context.Context, data map[string]interface{}) error {
	var err error = nil

	if atomic.LoadInt32(&p.status) == stop {
		err = ErrProducerClosed
	} else {
		jsonData, jsonErr := marshalToBytes(data)
		if jsonErr != nil {
			err = jsonErr
		} else {
			err = p.q.Put(jsonData)
		}
	}

	return err
}

func (p *AsyncProducer) Close(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&p.status, running, stop) {
		close(p.closeCh)
		if err := p.q.Close(); err != nil {
			DefaultLogger.Errorf("Close diskQ error : %s", err)
		}
		p.eg.Wait()
		return nil
	} else {
		return ErrProducerClosed
	}
}

func (p *AsyncProducer) init() error {

	p.eg.Go(p.runSender)

	DefaultLogger.Infof("ModeAsync staring, log path: %s", p.config.Directory)

	return nil
}

func (p *AsyncProducer) runSender() error {
	ingestSendIntervalTicker := time.NewTicker(p.config.SendInterval)

	var (
		lastCommitedAt time.Time
		msgs           [][]byte
		msgSize        int
	)

	reset := func() {
		lastCommitedAt = time.Now()
		msgs = [][]byte{}
		msgSize = 0
	}
	reset()

	send := func() error {
		clientMsgs := &client.Messages{}
		var msg client.Message
		for _, bytesMsg := range msgs {
			err := numberEncoding.Unmarshal(bytesMsg, &msg)
			if err != nil {
				return err
			}
			clientMsgs.Messages = append(clientMsgs.Messages, msg)
		}

		ctx, cancel := context.WithTimeout(context.Background(), p.config.SendTimeout)
		defer cancel()

		if err := p.ingestClient.Collect(ctx, clientMsgs); err != nil {
			DefaultLogger.Errorf("send data failed : %s", err)
			return err
		}

		reset()
		return nil
	}

	AppendAndCheckProcess := func(msgBytes []byte) error {
		if msgBytes != nil {
			if int64(msgSize+len(msgBytes)) > p.config.BatchSize {
				if err := send(); err != nil {
					return err
				}
			}

			msgs = append(msgs, msgBytes)
			msgSize += len(msgBytes)

			if len(msgs) >= p.config.MaxBufferRecords {
				if err := send(); err != nil {
					return err
				}
			}
		}
		return nil
	}

	defer func() {
		ingestSendIntervalTicker.Stop()
	}()

	for {
		select {
		case <-p.closeCh:
			DefaultLogger.Info("Sender receive close sig, exist")
			return nil
		case <-p.egCtx.Done():
			DefaultLogger.Info("Sender receive error sig, exist")
			return nil
		case <-ingestSendIntervalTicker.C:
			// 以下流程检测是否太久没有发送数据
			if time.Since(lastCommitedAt) >= p.config.SendInterval && len(msgs) > 0 {
				if err := send(); err != nil {
					return err
				}
			}
		case line := <-p.q.ReadChan():
			if err := AppendAndCheckProcess(line); err != nil {
				return err
			}
		}
	}
	return nil
}

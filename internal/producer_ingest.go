package internal

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	client "github.com/funny/ingest-client-go-sdk/v2"
)

type IngestProducerConfig struct {
	Mode             string
	IngestEndpoint   string
	AccessKey        string
	AccessSecret     string
	MaxBufferRecords int
	SendInterval     time.Duration
	SendTimeout      time.Duration
}

type IngestProducer struct {
	status       int32
	config       *IngestProducerConfig
	ingestClient *client.Client
	buffer       []*client.Message
	sendTimer    *time.Timer
	reportChan   chan *client.Message
	loopDie      chan struct{}
	loopExited   chan struct{}
}

func NewIngestProducer(config IngestProducerConfig) (Producer, error) {
	ingestClient, err := client.NewClient(client.Config{
		Endpoint:        config.IngestEndpoint,
		AccessKeyID:     config.AccessKey,
		AccessKeySecret: config.AccessSecret,
	})
	if err != nil {
		return nil, err
	}

	consumer := IngestProducer{
		status:       running,
		config:       &config,
		buffer:       make([]*client.Message, 0, config.MaxBufferRecords),
		ingestClient: ingestClient,
		sendTimer:    time.NewTimer(config.SendInterval),
		reportChan:   make(chan *client.Message),
		loopDie:      make(chan struct{}),
		loopExited:   make(chan struct{}),
	}

	go consumer.initConsumerLoop()

	DefaultLogger.Info("ModeSimple starting")

	return &consumer, nil
}

func (p *IngestProducer) Add(ctx context.Context, data map[string]interface{}) error {
	if atomic.LoadInt32(&p.status) != running {
		return ErrProducerClosed
	}

	// marshal early to detect errors
	b, err := marshalToBytes(data["data"])
	if err != nil {
		return err
	}

	msg := client.Message{
		Type: data["type"].(string),
		Data: json.RawMessage(b),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.reportChan <- &msg:
		return nil
	}
}

func (p *IngestProducer) Close(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&p.status, running, stop) {
		close(p.loopDie)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.loopExited:
			return nil
		}
	}
	return nil
}

func (p *IngestProducer) initConsumerLoop() {
	defer func() {
		close(p.loopExited)
	}()
	for {
		select {
		case <-p.loopDie:
			p.sendBatch()
			return
		case <-p.sendTimer.C:
			p.sendBatch()
		case data := <-p.reportChan:
			p.buffer = append(p.buffer, data)
			if len(p.buffer) >= p.config.MaxBufferRecords {
				p.sendBatch()
			}
		}
	}
}

func (p *IngestProducer) sendBatch() {
	p.sendTimer.Reset(p.config.SendInterval)
	if len(p.buffer) <= 0 {
		return
	}

	msgs := &client.Messages{}
	for _, msg := range p.buffer {
		msgs.Messages = append(msgs.Messages, *msg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.config.SendTimeout)
	defer cancel()

	if err := p.ingestClient.Collect(ctx, msgs); err != nil {
		DefaultLogger.Errorf("send data failed : %s", err)
	} else {
		p.buffer = make([]*client.Message, 0, p.config.MaxBufferRecords)
	}
}

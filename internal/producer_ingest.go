package internal

import (
	"context"
	"errors"
	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
	"sync/atomic"
	"time"
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
	buffer       []map[string]interface{}
	sendTimer    *time.Timer
	reportChan   chan map[string]interface{}
	loopDie      chan struct{}
	loopExited   chan struct{}
	statistician *statistician
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

	s, err := NewStatistician(config.Mode, config.IngestEndpoint, time.Minute)
	if err != nil && !errors.Is(err, ErrStatisticianIngestEndpointNotExist) {
		return nil, err
	}

	consumer := IngestProducer{
		status:       running,
		config:       &config,
		buffer:       make([]map[string]interface{}, 0, config.MaxBufferRecords),
		ingestClient: ingestClient,
		sendTimer:    time.NewTimer(config.SendInterval),
		reportChan:   make(chan map[string]interface{}),
		loopDie:      make(chan struct{}),
		loopExited:   make(chan struct{}),
		statistician: s,
	}

	go consumer.initConsumerLoop()

	DefaultLogger.Info("ModeSimple starting")

	return &consumer, nil
}

func (p *IngestProducer) Add(ctx context.Context, data map[string]interface{}) error {
	if atomic.LoadInt32(&p.status) != running {
		return ErrProducerClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.reportChan <- data:
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
		if p.statistician != nil {
			p.statistician.Close()
		}
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
	for _, dataMap := range p.buffer {
		msgs.Messages = append(msgs.Messages, client.Message{
			Type: dataMap["type"].(string),
			Data: dataMap["data"],
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.config.SendTimeout)
	defer cancel()

	if err := p.ingestClient.Collect(ctx, msgs); err != nil {
		DefaultLogger.Errorf("send data failed : %s", err)
	} else {
		p.buffer = make([]map[string]interface{}, 0, p.config.MaxBufferRecords)
		if p.statistician != nil {
			p.statistician.Count(getMsgEventTimeSortSlice(msgs))
		}
	}
}

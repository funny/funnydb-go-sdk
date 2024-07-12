package funnydb

import (
	"context"
	"errors"
	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
	"log"
	"sync/atomic"
	"time"
)

var ProducerCloseError = errors.New("producer closed")

const (
	running int32 = 1
	stop    int32 = 0
)

type ingestProducer struct {
	status       int32
	config       Config
	ingestClient *client.Client
	buffer       []M
	sendTimer    *time.Timer
	reportChan   chan M
	loopDie      chan struct{}
	loopExited   chan struct{}
}

func newIngestProducer(config Config) (producer, error) {

	ingestClient, err := client.NewClient(client.Config{
		Endpoint:        config.IngestEndpoint,
		AccessKeyID:     config.AccessKey,
		AccessKeySecret: config.AccessSecret,
	})
	if err != nil {
		return nil, err
	}

	consumer := ingestProducer{
		status:       running,
		config:       config,
		buffer:       make([]M, 0, config.MaxBufferRecords),
		ingestClient: ingestClient,
		sendTimer:    time.NewTimer(config.SendInterval),
		reportChan:   make(chan M),
		loopDie:      make(chan struct{}),
		loopExited:   make(chan struct{}),
	}

	go consumer.initConsumerLoop()

	return &consumer, nil
}

func (p *ingestProducer) Add(ctx context.Context, data M) error {
	if atomic.LoadInt32(&p.status) != running {
		return ProducerCloseError
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.reportChan <- data:
		return nil
	}
}

func (p *ingestProducer) Close(ctx context.Context) error {
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

func (p *ingestProducer) initConsumerLoop() {
	defer close(p.loopExited)
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

func (p *ingestProducer) sendBatch() {
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
		log.Printf("send data failed : %s", err)
	} else {
		p.buffer = make([]M, 0, p.config.MaxBufferRecords)
	}
}

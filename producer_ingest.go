package funnydb

import (
	"context"
	"fmt"
	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
	"log"
	"time"
)

type IngestProducer struct {
	config                *ClientConfig
	ingestClient          *client.Client
	buffer                []Reportable
	sendTimer             *time.Timer
	reportChan            chan *ingestAddRequest
	consumerLoopCloseChan chan context.Context
	consumerLoopFlushChan chan context.Context
}

type ingestAddRequest struct {
	ctx context.Context
	msg Reportable
}

func newIngestProducer(config *ClientConfig) (Producer, error) {
	return initIngestProducer(config)
}

func (p *IngestProducer) Add(ctx context.Context, data Reportable) error {
	p.reportChan <- &ingestAddRequest{msg: data, ctx: ctx}
	return nil
}

func (p *IngestProducer) Flush(ctx context.Context) error {
	p.consumerLoopFlushChan <- ctx
	return nil
}

func (p *IngestProducer) Close(ctx context.Context) error {
	p.consumerLoopCloseChan <- ctx
	return p.sendBatch(ctx)
}

func initIngestClient(config *ClientConfig) (*client.Client, error) {
	cfg := client.Config{
		Endpoint:        config.IngestEndpoint,
		AccessKeyID:     config.AccessKey,
		AccessKeySecret: config.AccessSecret,
	}
	return client.NewClient(cfg)
}

func initIngestProducer(config *ClientConfig) (*IngestProducer, error) {
	ingestClient, err := initIngestClient(config)
	if err != nil {
		return nil, err
	}

	consumer := IngestProducer{
		config:                config,
		buffer:                make([]Reportable, 0, config.MaxBufferSize),
		ingestClient:          ingestClient,
		sendTimer:             time.NewTimer(config.SendInterval),
		reportChan:            make(chan *ingestAddRequest),
		consumerLoopCloseChan: make(chan context.Context),
		consumerLoopFlushChan: make(chan context.Context),
	}

	go consumer.initConsumerLoop()

	return &consumer, nil
}

func (p *IngestProducer) initConsumerLoop() {
	for {
		select {
		case <-p.consumerLoopCloseChan:
			return
		case flushCtx := <-p.consumerLoopFlushChan:
			p.sendBatch(flushCtx)
		case <-p.sendTimer.C:
			p.sendBatch(context.Background())
		case req := <-p.reportChan:
			p.buffer = append(p.buffer, req.msg)
			if len(p.buffer) >= p.config.MaxBufferSize {
				p.sendBatch(req.ctx)
			}
		}
	}
}

func (p *IngestProducer) sendBatch(ctx context.Context) error {
	p.sendTimer.Reset(p.config.SendInterval)
	if len(p.buffer) <= 0 {
		return nil
	}

	msgs := &client.Messages{}

	for _, item := range p.buffer {
		dataMap, err := item.transformToReportableData()
		if err != nil {
			return fmt.Errorf("failed TransformToReportableData wher send batch : %s", err)
		}
		msgs.Messages = append(msgs.Messages, client.Message{
			Type: dataMap["type"].(string),
			Data: dataMap,
		})
	}

	ctx, cancel := context.WithTimeout(ctx, p.config.SendTimeout)
	defer cancel()

	if err := p.ingestClient.Collect(ctx, msgs); err != nil {
		return fmt.Errorf("failed to send batch : %s", err)
	}

	p.buffer = make([]Reportable, 0, p.config.MaxBufferSize)
	log.Println("发送数据成功")

	return nil
}

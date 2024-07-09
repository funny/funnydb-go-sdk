package funnydb

import (
	"context"
	"fmt"
	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
	"log"
	"time"
)

type IngestConsumer struct {
	config                *AnalyticsConfig
	client                *client.Client
	buffer                []Reportable
	sendTimer             *time.Timer
	reportChan            chan *ingestAddData
	consumerLoopCloseChan chan context.Context
	consumerLoopFlushChan chan context.Context
}

type ingestAddData struct {
	ctx context.Context
	msg Reportable
}

func newIngestConsumer(config *AnalyticsConfig) (Consumer, error) {
	return initIngestConsumer(config)
}

func (c *IngestConsumer) Add(ctx context.Context, data Reportable) error {
	c.reportChan <- &ingestAddData{msg: data, ctx: ctx}
	return nil
}

func (c *IngestConsumer) Flush(ctx context.Context) error {
	c.consumerLoopFlushChan <- ctx
	return nil
}

func (c *IngestConsumer) Close(ctx context.Context) error {
	c.consumerLoopCloseChan <- ctx
	return c.sendBatch(ctx)
}

func initIngestClient(config *AnalyticsConfig) (*client.Client, error) {
	cfg := client.Config{
		Endpoint:        config.IngestServerEndpoint,
		AccessKeyID:     config.IngestAccessKey,
		AccessKeySecret: config.IngestAccessSecret,
	}
	return client.NewClient(cfg)
}

func initIngestConsumer(config *AnalyticsConfig) (*IngestConsumer, error) {
	ingestClient, err := initIngestClient(config)
	if err != nil {
		return nil, err
	}

	consumer := IngestConsumer{
		config:                config,
		buffer:                make([]Reportable, 0, config.IngestMaxBufferSize),
		client:                ingestClient,
		sendTimer:             time.NewTimer(config.IngestMaxSendInterval),
		reportChan:            make(chan *ingestAddData),
		consumerLoopCloseChan: make(chan context.Context),
		consumerLoopFlushChan: make(chan context.Context),
	}

	go consumer.initConsumerLoop()

	return &consumer, nil
}

func (c *IngestConsumer) initConsumerLoop() {
	for {
		select {
		case <-c.consumerLoopCloseChan:
			return
		case flushCtx := <-c.consumerLoopFlushChan:
			c.sendBatch(flushCtx)
		case <-c.sendTimer.C:
			c.sendBatch(context.Background())
		case data := <-c.reportChan:
			c.buffer = append(c.buffer, data.msg)
			if len(c.buffer) >= c.config.IngestMaxBufferSize {
				c.sendBatch(data.ctx)
			}
		}
	}
}

func (c *IngestConsumer) sendBatch(ctx context.Context) error {
	c.sendTimer.Reset(c.config.IngestMaxSendInterval)
	if len(c.buffer) <= 0 {
		return nil
	}

	msgs := &client.Messages{}

	for _, item := range c.buffer {
		dataMap, err := item.TransformToReportableData()
		if err != nil {
			return fmt.Errorf("failed TransformToReportableData wher send batch : %s", err)
		}
		msgs.Messages = append(msgs.Messages, client.Message{
			Type: dataMap["type"].(string),
			Data: dataMap,
		})
	}

	ctx, cancel := context.WithTimeout(ctx, c.config.IngestSendTimeout)
	defer cancel()

	if err := c.client.Collect(ctx, msgs); err != nil {
		return fmt.Errorf("failed to send batch : %s", err)
	}

	c.buffer = make([]Reportable, 0, c.config.IngestMaxBufferSize)
	log.Println("发送数据成功")

	return nil
}

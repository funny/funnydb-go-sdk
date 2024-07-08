package funnydb

import (
	"context"
	"fmt"
	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type IngestConsumer struct {
	config                *AnalyticsConfig
	client                *client.Client
	buffer                []Reportable
	sendTimer             *time.Timer
	reportChan            chan Reportable
	consumerLoopCloseChan chan interface{}
	consumerLoopFlushChan chan interface{}
}

func newIngestConsumer(config *AnalyticsConfig) (Consumer, error) {
	return initIngestConsumer(config)
}

func (c *IngestConsumer) Add(data Reportable) error {
	c.reportChan <- data
	return nil
}

func (c *IngestConsumer) Flush() error {
	c.consumerLoopFlushChan <- true
	return nil
}

func (c *IngestConsumer) Close() error {
	c.consumerLoopCloseChan <- true
	return nil
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
		reportChan:            make(chan Reportable),
		consumerLoopCloseChan: make(chan interface{}),
		consumerLoopFlushChan: make(chan interface{}),
	}

	go consumer.initConsumerLoop()

	return &consumer, nil
}

func (c *IngestConsumer) initConsumerLoop() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			c.sendBatch()
			return
		case <-c.consumerLoopCloseChan:
			c.sendBatch()
			return
		case <-c.consumerLoopFlushChan:
			c.sendBatch()
		case <-c.sendTimer.C:
			c.sendBatch()
		case data := <-c.reportChan:
			c.buffer = append(c.buffer, data)
			if len(c.buffer) >= c.config.IngestMaxBufferSize {
				c.sendBatch()
			}
		}
	}
}

func (c *IngestConsumer) sendBatch() error {
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

	ctx, cancel := context.WithTimeout(context.Background(), c.config.IngestSendTimeout)
	defer cancel()

	if err := c.client.Collect(ctx, msgs); err != nil {
		return fmt.Errorf("failed to send batch : %s", err)
	}

	c.buffer = make([]Reportable, 0, c.config.IngestMaxBufferSize)
	log.Println("发送数据成功")

	return nil
}

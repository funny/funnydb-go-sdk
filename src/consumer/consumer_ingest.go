package consumer

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

const (
	DefaultServerUrl       = "http://localhost:8080"
	DefaultKey             = "demo"
	DefaultSecret          = "secret"
	DefaultMaxBufferSize   = 10
	DefaultMaxSendInterval = 5 * time.Second
	DefaultSendTimeout     = 5 * time.Second
)

type IngestConsumer struct {
	config                *IngestConsumerConfig
	client                *client.Client
	buffer                []Reportable
	sendTimer             *time.Timer
	reportChan            chan Reportable
	consumerLoopCloseChan chan interface{}
	consumerLoopFlushChan chan interface{}
}

type IngestConsumerConfig struct {
	serverUrl       string
	key             string
	secret          string
	maxBufferSize   int
	maxSendInterval time.Duration
	sendTimeout     time.Duration
}

func NewIngestConsumer(config *IngestConsumerConfig) (Consumer, error) {
	setDefaultParameters(config)
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

func setDefaultParameters(config *IngestConsumerConfig) {
	if config.serverUrl == "" {
		config.serverUrl = DefaultServerUrl
	}
	if config.key == "" {
		config.key = DefaultKey
	}
	if config.secret == "" {
		config.secret = DefaultSecret
	}
	if config.maxBufferSize <= 0 {
		config.maxBufferSize = DefaultMaxBufferSize
	}
	if config.maxSendInterval <= 0 {
		config.maxSendInterval = DefaultMaxSendInterval
	}
	if config.sendTimeout <= 0 {
		config.sendTimeout = DefaultSendTimeout
	}
}

func initIngestClient(config *IngestConsumerConfig) (*client.Client, error) {
	cfg := client.Config{
		Endpoint:        config.serverUrl,
		AccessKeyID:     config.key,
		AccessKeySecret: config.secret,
	}
	return client.NewClient(cfg)
}

func initIngestConsumer(config *IngestConsumerConfig) (*IngestConsumer, error) {
	ingestClient, err := initIngestClient(config)
	if err != nil {
		return nil, err
	}

	consumer := IngestConsumer{
		config:                config,
		buffer:                make([]Reportable, 0, config.maxBufferSize),
		client:                ingestClient,
		sendTimer:             time.NewTimer(config.maxSendInterval),
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
			log.Println("接收到 ctx 信号")
			c.sendBatch()
			return
		case <-c.consumerLoopCloseChan:
			log.Println("接收到关闭信号")
			c.sendBatch()
			return
		case <-c.consumerLoopFlushChan:
			log.Println("接收到清空信号")
			c.sendBatch()
		case <-c.sendTimer.C:
			log.Println("触发定时发送任务")
			c.sendBatch()
		case data := <-c.reportChan:
			log.Println("接收到数据")
			c.buffer = append(c.buffer, data)
			if len(c.buffer) >= c.config.maxBufferSize {
				c.sendBatch()
			}
		}
	}
}

func (c *IngestConsumer) sendBatch() error {
	c.sendTimer.Reset(c.config.maxSendInterval)
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

	ctx, cancel := context.WithTimeout(context.Background(), c.config.sendTimeout)
	defer cancel()

	if err := c.client.Collect(ctx, msgs); err != nil {
		return fmt.Errorf("failed to send batch : %s", err)
	}

	c.buffer = make([]Reportable, 0, c.config.maxBufferSize)
	log.Println("发送数据成功")

	return nil
}

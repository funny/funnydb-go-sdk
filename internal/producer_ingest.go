package internal

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

type IngestProducerConfig struct {
	IngestEndpoint   string        // 访问地址
	AccessKey        string        // 访问 key
	AccessSecret     string        // 访问秘钥
	MaxBufferRecords int           // 当缓存数据量超过该值，立刻发送这批数据到 ingest
	SendInterval     time.Duration // 当缓存数量达不到 MaxBufferSize，间隔一段时间也会发送数据到 ingest
	SendTimeout      time.Duration // 发送 ingest 请求超时时间
}

type IngestProducer struct {
	status       int32
	config       IngestProducerConfig
	ingestClient *client.Client
	buffer       []map[string]interface{}
	sendTimer    *time.Timer
	reportChan   chan map[string]interface{}
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
		config:       config,
		buffer:       make([]map[string]interface{}, 0, config.MaxBufferRecords),
		ingestClient: ingestClient,
		sendTimer:    time.NewTimer(config.SendInterval),
		reportChan:   make(chan map[string]interface{}),
		loopDie:      make(chan struct{}),
		loopExited:   make(chan struct{}),
	}

	go consumer.initConsumerLoop()

	return &consumer, nil
}

func (p *IngestProducer) Add(ctx context.Context, data map[string]interface{}) error {
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
		log.Printf("send data failed : %s", err)
	} else {
		p.buffer = make([]map[string]interface{}, 0, p.config.MaxBufferRecords)
	}
}

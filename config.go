package funnydb

import "time"

const (
	defaultProducerMode   = ProducerModeConsole
	defaultIngestEndpoint = "http://localhost:8080"
	defaultAccessKey      = "demo"
	defaultAccessSecret   = "secret"
	defaultMaxBufferSize  = 10
	defaultSendInterval   = 5 * time.Second
	defaultSendTimeout    = 5 * time.Second
)

type ClientConfig struct {
	ProducerMode string

	// ingest 相关参数
	IngestEndpoint string        // 访问地址
	AccessKey      string        // 访问 key
	AccessSecret   string        // 访问秘钥
	MaxBufferSize  int           // 当缓存数据量超过该值，立刻发送这批数据到 ingest
	SendInterval   time.Duration // 当缓存数量达不到 MaxBufferSize，间隔一段时间也会发送数据到 ingest
	SendTimeout    time.Duration // 发送 ingest 请求超时时间
}

func NewDefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ProducerMode:   defaultProducerMode,
		IngestEndpoint: defaultIngestEndpoint,
		AccessKey:      defaultAccessKey,
		AccessSecret:   defaultAccessSecret,
		MaxBufferSize:  defaultMaxBufferSize,
		SendInterval:   defaultSendInterval,
		SendTimeout:    defaultSendTimeout,
	}
}

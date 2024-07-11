package funnydb

import (
	"errors"
	"time"
)

type Mode string

const (
	ModeDebug  Mode = "console" // 结果打印到终端
	ModeSimple Mode = "ingest"  // 直接发送到服务端

	// 暂未实现
	// var ModeAsync Mode = "async"     // 存储到磁盘，异步发送
	// var ModePersistOnly Mode = "log" // 仅存储到磁盘

	DefaultMaxBufferRecords = 250
	DefaultSendInterval     = 100 * time.Millisecond
	DefaultSendTimeout      = 30 * time.Second
)

var UnknownProducerTypeError = errors.New("unknown producer type")
var ConfigIngestEndpointIllegalError = errors.New("producer config IngestEndpoint can not be empty")
var ConfigAccessKeyIllegalError = errors.New("producer config AccessKey can not be empty")
var ConfigAccessSecretIllegalError = errors.New("producer config AccessSecret can not be empty")

type Config struct {
	Mode Mode

	// ingest 相关参数
	IngestEndpoint   string        // 访问地址
	AccessKey        string        // 访问 key
	AccessSecret     string        // 访问秘钥
	MaxBufferRecords int           // 当缓存数据量超过该值，立刻发送这批数据到 ingest
	SendInterval     time.Duration // 当缓存数量达不到 MaxBufferSize，间隔一段时间也会发送数据到 ingest
	SendTimeout      time.Duration // 发送 ingest 请求超时时间
}

func (c *Config) checkConfig() error {
	switch c.Mode {
	case ModeDebug:
		return nil
	case ModeSimple:
		return c.checkIngestProducerConfigAndSetDefaultValue()
	default:
		return UnknownProducerTypeError
	}
}

func (c *Config) checkIngestProducerConfigAndSetDefaultValue() error {
	if c.IngestEndpoint == "" {
		return ConfigIngestEndpointIllegalError
	}
	if c.AccessKey == "" {
		return ConfigAccessKeyIllegalError
	}
	if c.AccessSecret == "" {
		return ConfigAccessSecretIllegalError
	}
	if c.MaxBufferRecords == 0 {
		c.MaxBufferRecords = DefaultMaxBufferRecords
	}
	if c.SendInterval == 0 {
		c.SendInterval = DefaultSendInterval
	}
	if c.SendTimeout == 0 {
		c.SendTimeout = DefaultSendTimeout
	}
	return nil
}

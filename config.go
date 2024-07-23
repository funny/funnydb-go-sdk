package funnydb

import (
	"errors"
	"git.sofunny.io/data-analysis/funnydb-go-sdk/internal"
	"time"
)

type Mode string

const (
	ModeDebug       Mode = "debug"        // 结果打印到终端
	ModeSimple      Mode = "simple"       // 直接发送到服务端
	ModePersistOnly Mode = "persist_only" // 仅存储到磁盘

	// 暂未实现
	// var ModeAsync Mode = "async"     // 存储到磁盘，异步发送

	DefaultMaxBufferRecords = 250
	DefaultSendInterval     = 100 * time.Millisecond
	DefaultSendTimeout      = 30 * time.Second
	DefaultLogFileSize      = 128
)

var ErrUnknownProducerType = errors.New("unknown producer type")
var ErrConfigIngestEndpointIllegal = errors.New("producer config IngestEndpoint can not be empty")
var ErrConfigAccessKeyIllegal = errors.New("producer config AccessKey can not be empty")
var ErrConfigAccessSecretIllegal = errors.New("producer config AccessSecret can not be empty")
var ErrConfigDirectoryIllegal = errors.New("producer config Directory can not be empty")

type Config struct {
	Mode Mode

	// ingest 相关参数
	IngestEndpoint   string        // 访问地址
	AccessKey        string        // 访问 key
	AccessSecret     string        // 访问秘钥
	MaxBufferRecords int           // 当缓存数据量超过该值，立刻发送这批数据到 ingest
	SendInterval     time.Duration // 当缓存数量达不到 MaxBufferSize，间隔一段时间也会发送数据到 ingest
	SendTimeout      time.Duration // 发送 ingest 请求超时时间

	// log 相关参数
	Directory string // 日志存储文件夹
	FileSize  int64  // 单个日志文件最大大小 (MB)
}

func (c *Config) checkConfig() error {
	switch c.Mode {
	case ModeDebug:
		return nil
	case ModeSimple:
		return c.checkIngestProducerConfigAndSetDefaultValue()
	case ModePersistOnly:
		return c.checkLogProducerConfigAndSetDefaultValue()
	default:
		return ErrUnknownProducerType
	}
}

func (c *Config) checkIngestProducerConfigAndSetDefaultValue() error {
	if c.IngestEndpoint == "" {
		return ErrConfigIngestEndpointIllegal
	}
	if c.AccessKey == "" {
		return ErrConfigAccessKeyIllegal
	}
	if c.AccessSecret == "" {
		return ErrConfigAccessSecretIllegal
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

func (c *Config) checkLogProducerConfigAndSetDefaultValue() error {
	if c.Directory == "" {
		return ErrConfigDirectoryIllegal
	}
	if c.FileSize == 0 {
		c.FileSize = DefaultLogFileSize
	}
	return nil
}

func (c *Config) generateIngestProducerConfig() *internal.IngestProducerConfig {
	return &internal.IngestProducerConfig{
		IngestEndpoint:   c.IngestEndpoint,
		AccessKey:        c.AccessKey,
		AccessSecret:     c.AccessSecret,
		MaxBufferRecords: c.MaxBufferRecords,
		SendInterval:     c.SendInterval,
		SendTimeout:      c.SendTimeout,
	}
}

func (c *Config) generateLogProducerConfig() *internal.LogProducerConfig {
	return &internal.LogProducerConfig{
		Directory: c.Directory,
		FileSize:  c.FileSize,
	}
}

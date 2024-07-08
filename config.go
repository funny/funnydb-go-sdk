package funnydb

import "time"

const (
	defaultIngestServerEndpoint  = "http://localhost:8080"
	defaultIngestAccessKey       = "demo"
	defaultIngestAccessSecret    = "secret"
	defaultIngestMaxBufferSize   = 10
	defaultIngestMaxSendInterval = 5 * time.Second
	defaultIngestSendTimeout     = 5 * time.Second
)

type AnalyticsConfig struct {
	IngestServerEndpoint  string
	IngestAccessKey       string
	IngestAccessSecret    string
	IngestMaxBufferSize   int
	IngestMaxSendInterval time.Duration
	IngestSendTimeout     time.Duration
}

func NewAnalyticsConfig() *AnalyticsConfig {
	return &AnalyticsConfig{
		IngestServerEndpoint:  defaultIngestServerEndpoint,
		IngestAccessKey:       defaultIngestAccessKey,
		IngestAccessSecret:    defaultIngestAccessSecret,
		IngestMaxBufferSize:   defaultIngestMaxBufferSize,
		IngestMaxSendInterval: defaultIngestMaxSendInterval,
		IngestSendTimeout:     defaultIngestSendTimeout,
	}
}

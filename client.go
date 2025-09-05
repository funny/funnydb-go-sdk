package funnydb

import (
	"context"

	"github.com/funny/funnydb-go-sdk/v2/internal"
)

type Client struct {
	p      internal.Producer
	config *Config
}

func NewClient(config *Config) (*Client, error) {
	var e error
	e = config.checkConfig()
	if e != nil {
		return nil, e
	}

	var p internal.Producer

	switch config.Mode {
	case ModeDebug:
		p, e = internal.NewConsoleProducer()
	case ModeSimple:
		p, e = internal.NewIngestProducer(*config.generateIngestProducerConfig())
	case ModePersistOnly:
		p, e = internal.NewLogProducer(*config.generateLogProducerConfig())
	case ModeAsync:
		p, e = internal.NewAsyncProducer(*config.generateAsyncProducerConfig())
	default:
		return nil, ErrUnknownProducerType
	}

	if e != nil {
		internal.DefaultLogger.Errorf("create sdk client error : %s", e)
		return nil, e
	}

	return &Client{p: p, config: config}, nil
}

func (c *Client) ReportEvent(ctx context.Context, e *Event) error {
	err := e.checkData()
	if err != nil {
		return err
	}
	data, err := e.transformToReportableData(c.config.Hostname)
	if err != nil {
		return err
	}
	return c.p.Add(ctx, data)
}

func (c *Client) ReportMutation(ctx context.Context, m *Mutation) error {
	err := m.checkData()
	if err != nil {
		return err
	}
	data, err := m.transformToReportableData(c.config.Hostname)
	if err != nil {
		return err
	}
	return c.p.Add(ctx, data)
}

func (c *Client) Close(ctx context.Context) error {
	err := c.p.Close(ctx)
	if err != nil {
		internal.DefaultLogger.Errorf("close client error: %s", err)
	} else {
		internal.DefaultLogger.Info("close client success")
	}
	return err
}

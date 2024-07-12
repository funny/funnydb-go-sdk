package funnydb

import (
	"context"
	"git.sofunny.io/data-analysis/funnydb-go-sdk/internal"
)

const (
	sdkType    = "go-sdk"
	sdkVersion = "1.0.0"

	dataFieldNameSdkType    = "#sdk_type"
	dataFieldNameSdkVersion = "#sdk_version"
	dataFieldNameEvent      = "#event"
	dataFieldNameTime       = "#time"
	dataFieldNameLogId      = "#log_id"
	dataFieldNameOperate    = "#operate"
	dataFieldNameIdentify   = "#identify"
	dataFieldNameProperties = "properties"
)

type Client struct {
	p internal.Producer
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
	default:
		return nil, UnknownProducerTypeError
	}

	if e != nil {
		return nil, e
	}

	return &Client{p}, nil
}

func (c *Client) ReportEvent(ctx context.Context, e *Event) error {
	err := e.checkData()
	if err != nil {
		return err
	}
	data, err := e.transformToReportableData()
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
	data, err := m.transformToReportableData()
	if err != nil {
		return err
	}
	return c.p.Add(ctx, data)
}

func (c *Client) Close(ctx context.Context) error {
	err := c.p.Close(ctx)
	return err
}

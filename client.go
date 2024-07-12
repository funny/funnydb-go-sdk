package funnydb

import (
	"context"
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
	p producer
}

func NewClient(config *Config) (*Client, error) {
	var e error
	e = config.checkConfig()
	if e != nil {
		return nil, e
	}

	var p producer

	switch config.Mode {
	case ModeDebug:
		p, e = newConsoleProducer(*config)
	case ModeSimple:
		p, e = newIngestProducer(*config)
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
	return c.report(ctx, e)
}

func (c *Client) ReportMutation(ctx context.Context, m *Mutation) error {
	err := m.checkData()
	if err != nil {
		return err
	}
	return c.report(ctx, m)
}

func (c *Client) report(ctx context.Context, r reportable) error {
	data, err := r.transformToReportableData()
	if err != nil {
		return err
	}
	return c.p.Add(ctx, data)
}

func (c *Client) Close(ctx context.Context) error {
	err := c.p.Close(ctx)
	return err
}

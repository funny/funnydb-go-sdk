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
	}

	if e != nil {
		return nil, e
	}

	return &Client{p}, nil
}

func (f *Client) ReportEvent(ctx context.Context, e *Event) error {
	err := e.checkData()
	if err != nil {
		return err
	}
	return f.report(ctx, e)
}

func (f *Client) ReportMutation(ctx context.Context, m *Mutation) error {
	err := m.checkData()
	if err != nil {
		return err
	}
	return f.report(ctx, m)
}

func (f *Client) report(ctx context.Context, r reportable) error {
	data, err := r.transformToReportableData()
	if err != nil {
		return err
	}
	return f.p.Add(ctx, data)
}

func (f *Client) Close(ctx context.Context) error {
	err := f.p.Close(ctx)
	return err
}

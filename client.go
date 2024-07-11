package funnydb

import (
	"context"
	"errors"
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

var UnknownProducerTypeError = errors.New("unknown producer type")

type Client struct {
	p Producer
}

func NewClient(config *ClientConfig) (*Client, error) {
	var p Producer
	var e error

	switch config.ProducerMode {
	case ProducerModeConsole:
		p, e = newConsoleProducer(config)
	case ProducerModeIngest:
		p, e = newIngestProducer(config)
	default:
		return nil, UnknownProducerTypeError
	}

	if e != nil {
		return nil, e
	}

	return &Client{p}, nil
}

func (f *Client) ReportTrace(ctx context.Context, event *Event) error {
	return f.p.Add(ctx, event)
}

func (f *Client) ReportMutation(ctx context.Context, mutation *Mutation) error {
	return f.p.Add(ctx, mutation)
}

func (f *Client) Close(ctx context.Context) error {
	err := f.p.Close(ctx)
	return err
}

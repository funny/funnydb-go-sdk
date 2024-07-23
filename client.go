package funnydb

import (
	"context"
	"git.sofunny.io/data-analysis/funnydb-go-sdk/internal"
	"log"
)

const (
	sdkType    = "go-sdk"
	sdkVersion = "1.1.0"

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
	case ModePersistOnly:
		p, e = internal.NewLogProducer(*config.generateLogProducerConfig())
	default:
		return nil, ErrUnknownProducerType
	}

	if e != nil {
		log.Printf("create sdk client error : %s\n", e)
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
	if err != nil {
		log.Printf("close client error: %s\n", err)
	} else {
		log.Printf("close client success")
	}
	return err
}

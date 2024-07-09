package funnydb

import (
	"context"
	"errors"
)

const (
	sdkType    = "go-sdk"
	sdkVersion = "1.0.0"

	ConsumerTypeConsole = "console"
	ConsumerTypeIngest  = "ingest"

	dataFieldNameSdkType    = "#sdk_type"
	dataFieldNameSdkVersion = "#sdk_version"
	dataFieldNameEvent      = "#event"
	dataFieldNameTime       = "#time"
	dataFieldNameLogId      = "#log_id"
	dataFieldNameOperate    = "#operate"
	dataFieldNameIdentify   = "#identify"
	dataFieldNameProperties = "properties"
)

var ErrorOfUnknownConsumerType = errors.New("unknown consumer type")

type Analytics struct {
	consumer Consumer
}

func NewFunnyDBAnalytics(consumerType string, config *AnalyticsConfig) (*Analytics, error) {
	var c Consumer
	var e error

	switch consumerType {
	case ConsumerTypeConsole:
		c, e = newConsoleConsumer(config)
	case ConsumerTypeIngest:
		c, e = newIngestConsumer(config)
	default:
		return nil, ErrorOfUnknownConsumerType
	}

	if e != nil {
		return nil, e
	}

	return &Analytics{consumer: c}, nil
}

func (f *Analytics) Report(ctx context.Context, data Reportable) error {
	return f.consumer.Add(ctx, data)
}

func (f *Analytics) Close(ctx context.Context) error {
	err := f.consumer.Close(ctx)
	return err
}

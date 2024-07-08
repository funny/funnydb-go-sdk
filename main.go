package main

import (
	"log"
)

const (
	SdkType    = "go-sdk"
	SdkVersion = "1.0.0"

	DataFieldNameSdkType    = "#sdk_type"
	DataFieldNameSdkVersion = "#sdk_version"
	DataFieldNameEvent      = "#event"
	DataFieldNameTime       = "#time"
	DataFieldNameLogId      = "#log_id"
	DataFieldNameOperate    = "#operate"
	DataFieldNameIdentify   = "#identify"
	DataFieldNameProperties = "properties"
)

type FunnyDBAnalytics struct {
	consumer Consumer
}

func NewConsoleAnalytics(config *ConsoleConsumerConfig) (*FunnyDBAnalytics, error) {
	consumer, err := NewConsoleConsumer(config)
	if err != nil {
		return nil, err
	}
	return &FunnyDBAnalytics{consumer: consumer}, nil
}

func NewIngestAnalytics(config *IngestConsumerConfig) (*FunnyDBAnalytics, error) {
	consumer, err := NewIngestConsumer(config)
	if err != nil {
		return nil, err
	}
	return &FunnyDBAnalytics{consumer: consumer}, nil
}

func (f *FunnyDBAnalytics) Report(data Reportable) error {
	return f.consumer.Add(data)
}

func (f *FunnyDBAnalytics) Close() error {
	err := f.consumer.Flush()
	if err != nil {
		log.Printf("Flush Error When Close Analytics: %s", err)
	}

	err = f.consumer.Close()
	return err
}

package sdk

import (
	"git.sofunny.io/data-analysis/funnydb-go-sdk/src/consumer"
	"log"
)

type FunnyDBAnalytics struct {
	consumer consumer.Consumer
}

func NewConsoleAnalytics(config *consumer.ConsoleConsumerConfig) (*FunnyDBAnalytics, error) {
	consumer, err := consumer.NewConsoleConsumer(config)
	if err != nil {
		return nil, err
	}
	return &FunnyDBAnalytics{consumer: consumer}, nil
}

func NewIngestAnalytics(config *consumer.IngestConsumerConfig) (*FunnyDBAnalytics, error) {
	consumer, err := consumer.NewIngestConsumer(config)
	if err != nil {
		return nil, err
	}
	return &FunnyDBAnalytics{consumer: consumer}, nil
}

func (f *FunnyDBAnalytics) Report(data consumer.Reportable) error {
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

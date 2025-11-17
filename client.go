package funnydb

import (
	"context"
	"fmt"
	"time"

	"github.com/funny/funnydb-go-sdk/v2/internal"
	"github.com/google/uuid"
)

type Client struct {
	p      internal.Producer
	config *Config
	stat   *StatCollector
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

	instanceID := uuid.Must(uuid.NewV7()).String()

	var stat *StatCollector
	if !config.DisableReportStats {
		var err error
		stat, err = newStatCollector(p, instanceID, config.Hostname, config.Mode, config.AccessKey, 30*time.Second)
		if err != nil {
			return nil, fmt.Errorf("create stat collector error: %s", err)
		}
	}

	return &Client{p: p, config: config, stat: stat}, nil
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
	if c.stat != nil {
		c.stat.Collect(e.Time, e.Name)
	}
	err = c.p.Add(ctx, data)
	if err != nil {
		return fmt.Errorf("ReportEvent: %s, event=%s time=%s", err, e.Name, e.Time)
	}
	return nil
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
	if c.stat != nil {
		c.stat.Collect(m.Time, m.Type)
	}
	err = c.p.Add(ctx, data)
	if err != nil {
		return fmt.Errorf("ReportMutation: %s, identity=%s time=%s", err, m.Identity, m.Time)
	}
	return nil
}

func (c *Client) Close(ctx context.Context) error {
	c.stat.Close()

	err := c.p.Close(ctx)
	if err != nil {
		internal.DefaultLogger.Errorf("close client error: %s", err)
	} else {
		internal.DefaultLogger.Info("close client success")
	}
	return err
}

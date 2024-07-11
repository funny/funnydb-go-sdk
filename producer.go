package funnydb

import (
	"context"
)

const (
	ProducerModeConsole = "console" // 打印到控制台
	ProducerModeIngest  = "ingest"  // 直接发送到 ingest
)

type Producer interface {
	Add(ctx context.Context, data Reportable) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}

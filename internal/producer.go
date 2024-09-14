package internal

import (
	"context"
	"errors"
)

var ErrProducerClosed = errors.New("producer has been closed")

const (
	EventTypeValue = "Event"

	SdkType    = "go-sdk"
	SdkVersion = "0.4.0"

	DataFieldNameSdkType    = "#sdk_type"
	DataFieldNameSdkVersion = "#sdk_version"
	DataFieldNameEvent      = "#event"
	DataFieldNameTime       = "#time"
	DataFieldNameLogId      = "#log_id"
	DataFieldNameOperate    = "#operate"
	DataFieldNameIdentify   = "#identify"
	DataFieldNameIp         = "#ip"
	DataFieldNameProperties = "properties"
)

const (
	running int32 = 1
	stop    int32 = 0
)

type Producer interface {
	Add(ctx context.Context, data map[string]interface{}) error
	Close(ctx context.Context) error
}

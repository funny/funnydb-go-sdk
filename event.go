package funnydb

import (
	"errors"
	"time"
)

const (
	EventTypeValue = "Event"
)

var EventDataNameIllegalError = errors.New("event data name can not be empty")

type Event struct {
	Name      string
	EventTime time.Time
	Props     M
}

func (e *Event) transformToReportableData() (M, error) {
	err := e.checkData()
	if err != nil {
		return nil, err
	}

	e.Props[dataFieldNameSdkType] = sdkType
	e.Props[dataFieldNameSdkVersion] = sdkVersion
	e.Props[dataFieldNameEvent] = e.Name
	e.Props[dataFieldNameTime] = e.EventTime.UnixMilli()

	logId, err := generateLogId()
	if err != nil {
		return nil, err
	}
	e.Props[dataFieldNameLogId] = logId

	return map[string]interface{}{
		"type": EventTypeValue,
		"data": e.Props,
	}, nil
}

func (e *Event) checkData() error {
	if e.Name == "" {
		return EventDataNameIllegalError
	}
	return nil
}

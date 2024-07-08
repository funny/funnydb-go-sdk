package funnydb

import (
	"time"
)

const (
	EventTypeValue = "Event"
)

type Event struct {
	Name       string
	ReportTime int64
	Props      M
}

func NewEvent(event string, props map[string]interface{}) Event {
	return Event{
		Name:       event,
		ReportTime: time.Now().UnixMilli(),
		Props:      props,
	}
}

func (e *Event) TransformToReportableData() (M, error) {
	e.Props[dataFieldNameSdkType] = sdkType
	e.Props[dataFieldNameSdkVersion] = sdkVersion
	e.Props[dataFieldNameEvent] = e.Name
	e.Props[dataFieldNameTime] = e.ReportTime

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

package main

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
	e.Props[DataFieldNameSdkType] = SdkType
	e.Props[DataFieldNameSdkVersion] = SdkVersion
	e.Props[DataFieldNameEvent] = e.Name
	e.Props[DataFieldNameTime] = e.ReportTime

	logId, err := GenerateLogId()
	if err != nil {
		return nil, err
	}
	e.Props[DataFieldNameLogId] = logId

	return map[string]interface{}{
		"type": EventTypeValue,
		"data": e.Props,
	}, nil
}

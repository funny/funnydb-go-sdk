package consumer

import (
	"git.sofunny.io/data-analysis/funnydb-go-sdk/src/utils"
	"time"
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
	e.Props["#sdk_type"] = utils.SDK_TYPE
	e.Props["#sdk_version"] = utils.SDK_VERSION
	e.Props["#event"] = e.Name
	e.Props["#time"] = e.ReportTime

	logId, err := utils.GenerateLogId()
	if err != nil {
		return nil, err
	}
	e.Props["#log_id"] = logId

	return map[string]interface{}{
		"type": "Event",
		"data": e.Props,
	}, nil
}

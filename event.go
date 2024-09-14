package funnydb

import (
	"errors"
	"git.sofunny.io/data-analysis/funnydb-go-sdk/internal"
	"time"
)

var ErrEventDataNameIllegal = errors.New("event data name can not be empty")

type Event struct {
	Name  string
	Time  time.Time
	Props map[string]interface{}
}

func (e *Event) transformToReportableData() (map[string]interface{}, error) {

	e.Props[internal.DataFieldNameSdkType] = internal.SdkType
	e.Props[internal.DataFieldNameSdkVersion] = internal.SdkVersion
	e.Props[internal.DataFieldNameEvent] = e.Name

	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	e.Props[internal.DataFieldNameTime] = e.Time.UnixMilli()

	logId, err := internal.GenerateLogId()
	if err != nil {
		return nil, err
	}
	e.Props[internal.DataFieldNameLogId] = logId

	return map[string]interface{}{
		"type": internal.EventTypeValue,
		"data": e.Props,
	}, nil
}

func (e *Event) checkData() error {
	if e.Name == "" {
		return ErrEventDataNameIllegal
	}
	return nil
}

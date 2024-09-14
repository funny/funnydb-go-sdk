package funnydb

import (
	"git.sofunny.io/data-analysis/funnydb-go-sdk/internal"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEvent_transformToReportableData(t *testing.T) {
	eventName := "UserLogin"
	eventTime := time.Now()
	eventProps := map[string]interface{}{
		"field1": 1,
		"field2": "2",
	}

	e := &Event{
		Name:  eventName,
		Time:  eventTime,
		Props: eventProps,
	}
	reportableData, err := e.transformToReportableData()
	assert.Nil(t, err)

	assert.Equal(t, internal.EventTypeValue, reportableData["type"].(string))
	dataMap := reportableData["data"].(map[string]interface{})
	assert.Equal(t, internal.SdkType, dataMap[internal.DataFieldNameSdkType].(string))
	assert.Equal(t, internal.SdkVersion, dataMap[internal.DataFieldNameSdkVersion].(string))
	assert.Equal(t, eventName, dataMap[internal.DataFieldNameEvent].(string))
	assert.Equal(t, eventTime.UnixMilli(), dataMap[internal.DataFieldNameTime].(int64))
	assert.NotEmpty(t, dataMap[internal.DataFieldNameLogId].(string))
	assert.Equal(t, 1, dataMap["field1"].(int))
	assert.Equal(t, "2", dataMap["field2"].(string))

	// 校验不传入 Time 时，自动填充当前时间
	e2 := &Event{
		Name:  eventName,
		Props: eventProps,
	}
	reportableData2, err := e2.transformToReportableData()
	assert.Nil(t, err)

	dataMap2 := reportableData2["data"].(map[string]interface{})
	assert.Equal(t, true, dataMap2[internal.DataFieldNameTime].(int64) >= eventTime.UnixMilli())
}

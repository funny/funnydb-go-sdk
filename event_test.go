package funnydb

import (
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

	assert.Equal(t, EventTypeValue, reportableData["type"].(string))
	dataMap := reportableData["data"].(map[string]interface{})
	assert.Equal(t, sdkType, dataMap[dataFieldNameSdkType].(string))
	assert.Equal(t, sdkVersion, dataMap[dataFieldNameSdkVersion].(string))
	assert.Equal(t, eventName, dataMap[dataFieldNameEvent].(string))
	assert.Equal(t, eventTime.UnixMilli(), dataMap[dataFieldNameTime].(int64))
	assert.NotEmpty(t, dataMap[dataFieldNameLogId].(string))
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
	assert.Equal(t, true, dataMap2[dataFieldNameTime].(int64) >= eventTime.UnixMilli())
}

package funnydb

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMutation_transformToReportableData(t *testing.T) {
	identity := "user-id-1"
	eventTime := time.Now()
	mutationProps := map[string]interface{}{
		"field1": 1,
		"field2": "2",
	}

	m := &Mutation{
		Time:     eventTime,
		Type:     MutationTypeUser,
		Identity: identity,
		Operate:  OperateTypeSet,
		Props:    mutationProps,
	}

	reportableData, err := m.transformToReportableData()
	assert.Nil(t, err)

	assert.Equal(t, MutationTypeUser, reportableData["type"].(string))
	dataMap := reportableData["data"].(map[string]interface{})
	assert.Equal(t, sdkType, dataMap[dataFieldNameSdkType].(string))
	assert.Equal(t, sdkVersion, dataMap[dataFieldNameSdkVersion].(string))
	assert.Equal(t, eventTime.UnixMilli(), dataMap[dataFieldNameTime].(int64))
	assert.NotEmpty(t, dataMap[dataFieldNameLogId].(string))
	assert.Equal(t, OperateTypeSet, dataMap[dataFieldNameOperate].(string))
	assert.Equal(t, identity, dataMap[dataFieldNameIdentify].(string))
	propertiesMap := dataMap[dataFieldNameProperties].(map[string]interface{})
	assert.Equal(t, 1, propertiesMap["field1"].(int))
	assert.Equal(t, "2", propertiesMap["field2"].(string))

	// 校验不传入 Time 时，自动填充当前时间
	m2 := &Mutation{
		Type:     MutationTypeUser,
		Identity: identity,
		Operate:  OperateTypeSet,
		Props:    mutationProps,
	}
	reportableData2, err := m2.transformToReportableData()
	assert.Nil(t, err)

	dataMap2 := reportableData2["data"].(map[string]interface{})
	assert.Equal(t, true, dataMap2[dataFieldNameTime].(int64) >= eventTime.UnixMilli())
}
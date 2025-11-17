package funnydb

import (
	"testing"
	"time"

	"github.com/funny/funnydb-go-sdk/v2/internal"
	"github.com/stretchr/testify/assert"
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

	reportableData, err := m.transformToReportableData("localhost")
	assert.Nil(t, err)

	assert.Equal(t, MutationTypeUser, reportableData["type"].(string))
	dataMap := reportableData["data"].(map[string]interface{})
	assert.Equal(t, internal.SdkType, dataMap[internal.DataFieldNameSdkType].(string))
	assert.Equal(t, internal.SdkVersion, dataMap[internal.DataFieldNameSdkVersion].(string))
	assert.Equal(t, eventTime.UnixMilli(), dataMap[internal.DataFieldNameTime].(int64))
	assert.NotEmpty(t, dataMap[internal.DataFieldNameLogId].(string))
	assert.Equal(t, OperateTypeSet, dataMap[internal.DataFieldNameOperate].(string))
	assert.Equal(t, identity, dataMap[internal.DataFieldNameIdentify].(string))
	propertiesMap := dataMap[internal.DataFieldNameProperties].(map[string]interface{})
	assert.Equal(t, 1, propertiesMap["field1"].(int))
	assert.Equal(t, "2", propertiesMap["field2"].(string))

	// 校验不传入 Time 时，自动填充当前时间
	m2 := &Mutation{
		Type:     MutationTypeUser,
		Identity: identity,
		Operate:  OperateTypeSet,
		Props:    mutationProps,
	}
	reportableData2, err := m2.transformToReportableData("localhost")
	assert.Nil(t, err)

	dataMap2 := reportableData2["data"].(map[string]interface{})
	assert.Equal(t, true, dataMap2[internal.DataFieldNameTime].(int64) >= eventTime.UnixMilli())
}

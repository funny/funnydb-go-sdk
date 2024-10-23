package internal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewStatistician(t *testing.T) {
	cnStatistician, err := NewStatistician("async", "accessKeyId", IngestCnEndpoint, time.Minute, time.Hour)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)
	cnStatistician.Close()

	sgStatistician, err := NewStatistician("async", "accessKeyId", IngestSgEndpoint, time.Minute, time.Hour)
	assert.Nil(t, err)
	assert.NotNil(t, sgStatistician)
	sgStatistician.Close()

	nilStatistician, err := NewStatistician("async", "accessKeyId", IngestMockEndpoint, time.Minute, time.Hour)
	assert.Equal(t, ErrStatisticianIngestEndpointNotExist, err)
	assert.Nil(t, nilStatistician)
}

func TestStatisticianReportSuccess(t *testing.T) {
	cnStatistician, err := NewStatistician("async", "accessKeyId", IngestCnEndpoint, time.Minute, time.Hour)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	// 模拟并发使用
	count := 1000
	msgEventTimeSlice := []int64{time.Now().UnixMilli()}
	for i := 0; i < count; i++ {
		t.Run(fmt.Sprintf("StatisticianReportAdd-%d", i), func(t *testing.T) {
			cnStatistician.Count(msgEventTimeSlice)
		})
	}

	bodyMap := map[string]interface{}{
		DataFieldNameSdkType:          SdkType,
		DataFieldNameSdkVersion:       SdkVersion,
		DataFieldNameEvent:            StatsEventName,
		DataFieldNameIp:               cnStatistician.instanceIp,
		StatsDataFieldNameHostname:    cnStatistician.instanceHostname,
		StatsDataFieldNameInstanceId:  cnStatistician.instanceId,
		StatsDataFieldNameMode:        cnStatistician.initMode,
		StatsDataFieldNameAccessKeyId: cnStatistician.accessKeyId,
		// 这里转换成 float64 主要是由于 jsoniter.Unmarshal 反序列化后数字都采用 float64 表示
		StatsDataFieldNameInitTime:    float64(cnStatistician.initTime),
		StatsDataFieldNameBeginTime:   float64(cnStatistician.beginTime),
		StatsDataFieldNameEndTime:     float64(cnStatistician.endTime),
		StatsDataFieldNameReportTotal: float64(count),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 关闭前会提交数据
	cnStatistician.Close()

	WaitingForGockDone(t)
}

// 测试由调用接口 count 触发的当前这一小时的数据上报
func TestStatisticianReportCrossBorderByCount(t *testing.T) {
	cnStatistician, err := NewStatistician("async", "accessKeyId", IngestCnEndpoint, time.Hour*24, time.Hour)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	now := time.Now()
	currentHourTime := now.Truncate(time.Hour)
	nextHourTime := currentHourTime.Add(time.Hour)
	crossEndTime := nextHourTime.Add(time.Hour)

	var currentHourMsgEventTimeSlice = []int64{currentHourTime.UnixMilli()}
	cnStatistician.Count(currentHourMsgEventTimeSlice)

	bodyMap := map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(currentHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(nextHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(len(currentHourMsgEventTimeSlice)),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 假设时间点到达下一个小时，会触发数据提交
	var nextHourMsgEventTimeSlice = []int64{nextHourTime.UnixMilli()}
	cnStatistician.Count(nextHourMsgEventTimeSlice)

	WaitingForGockDone(t)

	bodyMap = map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(nextHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(crossEndTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(len(nextHourMsgEventTimeSlice)),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 关闭前会提交数据
	cnStatistician.Close()

	WaitingForGockDone(t)
}

// 测试由定期上报机制触发的当前这一小时的数据上报
func TestStatisticianReportCrossBorderByReportInterval(t *testing.T) {
	now := time.Now()
	currentHourTime := now.Truncate(time.Hour)
	lastHourTime := currentHourTime.Add(-time.Hour)
	nextHourTime := currentHourTime.Add(time.Hour)

	reportInterval := time.Second * 3

	cnStatistician, err := createStatistician("async", "accessKeyId", IngestCnEndpoint, reportInterval, lastHourTime, time.Hour)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	var lastHourMsgEventTimeSlice = []int64{lastHourTime.UnixMilli()}
	cnStatistician.Count(lastHourMsgEventTimeSlice)

	bodyMap := map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(lastHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(currentHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(len(lastHourMsgEventTimeSlice)),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 等待触发定期上报
	time.Sleep(reportInterval * 2)

	WaitingForGockDone(t)

	var currentHourMsgEventTimeSlice = []int64{currentHourTime.UnixMilli()}
	cnStatistician.Count(currentHourMsgEventTimeSlice)

	bodyMap = map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(currentHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(nextHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(len(currentHourMsgEventTimeSlice)),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 关闭前会提交数据
	cnStatistician.Close()

	WaitingForGockDone(t)
}

// 测试一批数据中，事件时间跨越小时级别，也能正常上报
func TestStatisticianReportBatchCrossHour(t *testing.T) {
	now := time.Now()
	currentHourTime := now.Truncate(time.Hour)
	nextHourTime := currentHourTime.Add(time.Hour)
	nextTwoHourTime := nextHourTime.Add(time.Hour)

	cnStatistician, err := createStatistician("async", "accessKeyId", IngestCnEndpoint, time.Hour*24, currentHourTime, time.Hour)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	bodyMap := map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(currentHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(nextHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(1),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 事件时间颠倒了，也会自动排序后正常执行
	var crossHourMsgEventTimeSlice = []int64{nextHourTime.UnixMilli(), currentHourTime.UnixMilli()}
	cnStatistician.Count(crossHourMsgEventTimeSlice)

	WaitingForGockDone(t)

	bodyMap = map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(nextHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(nextTwoHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(1),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 关闭前会提交数据
	cnStatistician.Close()

	WaitingForGockDone(t)
}

// 测试多次重启改变统计区间
func TestStatisticianChangeStatisticalInterval(t *testing.T) {
	now := time.Now()
	oneDayReportInterval := time.Hour * 24

	fiveMinuteStatisticalInterval := time.Minute * 5
	currentFiveMinuteTime := now.Truncate(fiveMinuteStatisticalInterval)
	nextFiveMinuteTime := currentFiveMinuteTime.Add(fiveMinuteStatisticalInterval)

	bodyMap := map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(currentFiveMinuteTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(nextFiveMinuteTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(1),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	cnStatistician, err := createStatistician("async", "accessKeyId", IngestCnEndpoint, oneDayReportInterval, now, fiveMinuteStatisticalInterval)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	cnStatistician.Count([]int64{now.UnixMilli()})
	cnStatistician.Close() // 关闭触发上报

	WaitingForGockDone(t)

	halfHourStatisticalInterval := time.Minute * 30
	currentHalfHourTime := now.Truncate(halfHourStatisticalInterval)
	nextHalfHourTime := currentHalfHourTime.Add(halfHourStatisticalInterval)

	bodyMap = map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(currentHalfHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(nextHalfHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(1),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	cnStatistician, err = createStatistician("async", "accessKeyId", IngestCnEndpoint, oneDayReportInterval, now, halfHourStatisticalInterval)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	cnStatistician.Count([]int64{now.UnixMilli()})
	cnStatistician.Close() // 关闭触发上报

	WaitingForGockDone(t)

	oneHourStatisticalInterval := time.Hour
	currentHourTime := now.Truncate(oneHourStatisticalInterval)
	nextHourTime := currentHourTime.Add(oneHourStatisticalInterval)

	bodyMap = map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(currentHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(nextHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(1),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	cnStatistician, err = createStatistician("async", "accessKeyId", IngestCnEndpoint, oneDayReportInterval, now, oneHourStatisticalInterval)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	cnStatistician.Count([]int64{now.UnixMilli()})
	cnStatistician.Close() // 关闭触发上报

	WaitingForGockDone(t)

}

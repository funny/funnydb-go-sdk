package internal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const TestEventName = "login"

func NewTestStatsGroup(beginTime time.Time, statisticalInterval time.Duration) StatsGroup {
	return NewStatsGroup(beginTime, statisticalInterval, TestEventName)
}

func TestNewStatistician(t *testing.T) {
	reportInterval := time.Minute
	statisticalInterval := time.Hour

	cnStatistician, err := NewStatistician("async", "accessKeyId", IngestCnEndpoint, reportInterval, statisticalInterval)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)
	cnStatistician.Close()

	sgStatistician, err := NewStatistician("async", "accessKeyId", IngestSgEndpoint, reportInterval, statisticalInterval)
	assert.Nil(t, err)
	assert.NotNil(t, sgStatistician)
	sgStatistician.Close()

	nilStatistician, err := NewStatistician("async", "accessKeyId", IngestMockEndpoint, reportInterval, statisticalInterval)
	assert.Equal(t, ErrStatisticianIngestEndpointNotExist, err)
	assert.Nil(t, nilStatistician)
}

// 测试并发上报正确性
func TestStatisticianReportSuccess(t *testing.T) {
	reportInterval := time.Hour
	statisticalInterval := time.Hour

	cnStatistician, err := NewStatistician("async", "accessKeyId", IngestCnEndpoint, reportInterval, statisticalInterval)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	// 模拟并发使用
	statsGroup := NewTestStatsGroup(time.Now(), cnStatistician.statisticalInterval)
	count := 1000
	s := []StatsGroup{statsGroup}
	for i := 0; i < count; i++ {
		t.Run(fmt.Sprintf("StatisticianReportAdd-%d", i), func(t *testing.T) {
			cnStatistician.Count(s)
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
		StatsDataFieldNameBeginTime:   float64(statsGroup.beginTimeMils),
		StatsDataFieldNameEndTime:     float64(statsGroup.endTimeMils),
		StatsDataFieldNameEvent:       statsGroup.event,
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

// 测试由事件时间触发的数据上报正确性
func TestStatisticianReportCrossBorderByCount(t *testing.T) {
	reportInterval := time.Hour * 24
	statisticalInterval := time.Hour

	cnStatistician, err := NewStatistician("async", "accessKeyId", IngestCnEndpoint, reportInterval, statisticalInterval)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	now := time.Now()
	currentHourTime := now.Truncate(time.Hour)
	nextHourTime := currentHourTime.Add(time.Hour)

	statsGroup := NewTestStatsGroup(currentHourTime, cnStatistician.statisticalInterval)
	var reportData = []StatsGroup{statsGroup}
	cnStatistician.Count(reportData)

	bodyMap := map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(statsGroup.beginTimeMils),
		StatsDataFieldNameEndTime:     float64(statsGroup.endTimeMils),
		StatsDataFieldNameEvent:       statsGroup.event,
		StatsDataFieldNameReportTotal: float64(len(reportData)),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 假设时间点到达下一个小时，会触发数据提交
	nextHourStatsGroup := NewTestStatsGroup(nextHourTime, cnStatistician.statisticalInterval)
	var nextReportData = []StatsGroup{nextHourStatsGroup}
	cnStatistician.Count(nextReportData)

	WaitingForGockDone(t)

	bodyMap = map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(nextHourStatsGroup.beginTimeMils),
		StatsDataFieldNameEndTime:     float64(nextHourStatsGroup.endTimeMils),
		StatsDataFieldNameEvent:       nextHourStatsGroup.event,
		StatsDataFieldNameReportTotal: float64(len(nextReportData)),
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

// 测试由定时上报触发的数据上报正确性
func TestStatisticianReportCrossBorderByReportInterval(t *testing.T) {
	now := time.Now()
	currentHourTime := now.Truncate(time.Hour)
	lastHourTime := currentHourTime.Add(-time.Hour)

	reportInterval := time.Second * 3
	appInitTime := lastHourTime
	statisticalInterval := time.Hour

	cnStatistician, err := createStatistician("async", "accessKeyId", IngestCnEndpoint, reportInterval, appInitTime, statisticalInterval)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	statsGroup := NewTestStatsGroup(lastHourTime, cnStatistician.statisticalInterval)
	var reportData = []StatsGroup{statsGroup}
	cnStatistician.Count(reportData)

	bodyMap := map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(statsGroup.beginTimeMils),
		StatsDataFieldNameEndTime:     float64(statsGroup.endTimeMils),
		StatsDataFieldNameEvent:       statsGroup.event,
		StatsDataFieldNameReportTotal: float64(len(reportData)),
	}

	CreateCnCollectGockReq().
		SetMatcher(GenerateMessageDataCheckMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 等待触发定期上报
	time.Sleep(reportInterval * 2)

	WaitingForGockDone(t)

	currentHourStatsGroup := NewTestStatsGroup(currentHourTime, cnStatistician.statisticalInterval)
	var currentReportData = []StatsGroup{currentHourStatsGroup}
	cnStatistician.Count(currentReportData)

	bodyMap = map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(currentHourStatsGroup.beginTimeMils),
		StatsDataFieldNameEndTime:     float64(currentHourStatsGroup.endTimeMils),
		StatsDataFieldNameEvent:       currentHourStatsGroup.event,
		StatsDataFieldNameReportTotal: float64(len(currentReportData)),
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

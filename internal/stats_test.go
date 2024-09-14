package internal

import (
	"fmt"
	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
	"github.com/h2non/gock"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"testing"
	"time"
)

func createGockReq() *gock.Request {
	return gock.New("https://ingest.zh-cn.xmfunny.com").
		Post("/v1/collect")
}

func waitingForGockDone(t *testing.T) {
	for {
		if gock.IsDone() {
			break
		}
		if gock.HasUnmatchedRequest() {
			t.Fatal("Has Unmatched Request")
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func generateSingleMessageBodyMatcher(m map[string]interface{}) *gock.MockMatcher {
	matcher := gock.NewBasicMatcher()

	matcher.Add(func(req *http.Request, ereq *gock.Request) (bool, error) {
		bytes, matcherErr := io.ReadAll(req.Body)
		if matcherErr != nil {
			return false, matcherErr
		}

		gunzipData, unzipErr := GunzipData(bytes)
		if unzipErr != nil {
			return false, unzipErr
		}

		var batch client.Messages
		unmarshalErr := jsoniter.Unmarshal(gunzipData, &batch)
		if unmarshalErr != nil {
			return false, unmarshalErr
		}

		if len(batch.Messages) != 1 {
			return false, nil
		}

		message := batch.Messages[0]
		if message.Type != EventTypeValue {
			return false, nil
		}

		dataMap, ok := message.Data.(map[string]interface{})
		if !ok {
			return false, nil
		}

		for fieldKey, fieldValue := range m {
			if fieldValue != dataMap[fieldKey] {
				return false, nil
			}
		}

		return true, nil
	})

	return matcher
}

func TestNewStatistician(t *testing.T) {
	cnStatistician, err := NewStatistician("async", "https://ingest.zh-cn.xmfunny.com", time.Minute)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)
	cnStatistician.Close()

	sgStatistician, err := NewStatistician("async", "https://ingest.sg.xmfunny.com", time.Minute)
	assert.Nil(t, err)
	assert.NotNil(t, sgStatistician)
	sgStatistician.Close()

	nilStatistician, err := NewStatistician("async", "https://miss.com", time.Minute)
	assert.Equal(t, ErrStatisticianIngestEndpointNotExist, err)
	assert.Nil(t, nilStatistician)
}

func TestStatisticianReportSuccess(t *testing.T) {
	cnStatistician, err := NewStatistician("async", "https://ingest.zh-cn.xmfunny.com", time.Minute)
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
		DataFieldNameSdkType:         SdkType,
		DataFieldNameSdkVersion:      SdkVersion,
		DataFieldNameEvent:           StatsEventName,
		DataFieldNameIp:              cnStatistician.instanceIp,
		StatsDataFieldNameHostname:   cnStatistician.instanceHostname,
		StatsDataFieldNameInstanceId: cnStatistician.instanceId,
		StatsDataFieldNameMode:       cnStatistician.initMode,
		// 这里转换成 float64 主要是由于 jsoniter.Unmarshal 反序列化后数字都采用 float64 表示
		StatsDataFieldNameInitTime:    float64(cnStatistician.initTime),
		StatsDataFieldNameBeginTime:   float64(cnStatistician.beginTime),
		StatsDataFieldNameEndTime:     float64(cnStatistician.endTime),
		StatsDataFieldNameReportTotal: float64(count),
	}

	createGockReq().
		SetMatcher(generateSingleMessageBodyMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 关闭前会提交数据
	cnStatistician.Close()

	waitingForGockDone(t)
}

// 测试由调用接口 count 触发的当前这一小时的数据上报
func TestStatisticianReportCrossBorderByCount(t *testing.T) {
	cnStatistician, err := NewStatistician("async", "https://ingest.zh-cn.xmfunny.com", time.Hour*24)
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

	createGockReq().
		SetMatcher(generateSingleMessageBodyMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 假设时间点到达下一个小时，会触发数据提交
	var nextHourMsgEventTimeSlice = []int64{nextHourTime.UnixMilli()}
	cnStatistician.Count(nextHourMsgEventTimeSlice)

	waitingForGockDone(t)

	bodyMap = map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(nextHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(crossEndTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(len(nextHourMsgEventTimeSlice)),
	}

	createGockReq().
		SetMatcher(generateSingleMessageBodyMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 关闭前会提交数据
	cnStatistician.Close()

	waitingForGockDone(t)
}

// 测试由定期上报机制触发的当前这一小时的数据上报
func TestStatisticianReportCrossBorderByReportInterval(t *testing.T) {
	now := time.Now()
	currentHourTime := now.Truncate(time.Hour)
	lastHourTime := currentHourTime.Add(-time.Hour)
	nextHourTime := currentHourTime.Add(time.Hour)

	reportInterval := time.Second * 3

	cnStatistician, err := createStatistician("async", "https://ingest.zh-cn.xmfunny.com", reportInterval, lastHourTime)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	var lastHourMsgEventTimeSlice = []int64{lastHourTime.UnixMilli()}
	cnStatistician.Count(lastHourMsgEventTimeSlice)

	bodyMap := map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(lastHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(currentHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(len(lastHourMsgEventTimeSlice)),
	}

	createGockReq().
		SetMatcher(generateSingleMessageBodyMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 等待触发定期上报
	time.Sleep(reportInterval * 2)

	waitingForGockDone(t)

	var currentHourMsgEventTimeSlice = []int64{currentHourTime.UnixMilli()}
	cnStatistician.Count(currentHourMsgEventTimeSlice)

	bodyMap = map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(currentHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(nextHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(len(currentHourMsgEventTimeSlice)),
	}

	createGockReq().
		SetMatcher(generateSingleMessageBodyMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 关闭前会提交数据
	cnStatistician.Close()

	waitingForGockDone(t)
}

// 测试一批数据中，事件时间跨越小时级别，也能正常上报
func TestStatisticianReportBatchCrossHour(t *testing.T) {
	now := time.Now()
	currentHourTime := now.Truncate(time.Hour)
	nextHourTime := currentHourTime.Add(time.Hour)
	nextTwoHourTime := nextHourTime.Add(time.Hour)

	cnStatistician, err := createStatistician("async", "https://ingest.zh-cn.xmfunny.com", time.Hour*24, currentHourTime)
	assert.Nil(t, err)
	assert.NotNil(t, cnStatistician)

	bodyMap := map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(currentHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(nextHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(1),
	}

	createGockReq().
		SetMatcher(generateSingleMessageBodyMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 事件时间颠倒了，也会自动排序后正常执行
	var crossHourMsgEventTimeSlice = []int64{nextHourTime.UnixMilli(), currentHourTime.UnixMilli()}
	cnStatistician.Count(crossHourMsgEventTimeSlice)

	waitingForGockDone(t)

	bodyMap = map[string]interface{}{
		StatsDataFieldNameBeginTime:   float64(nextHourTime.UnixMilli()),
		StatsDataFieldNameEndTime:     float64(nextTwoHourTime.UnixMilli()),
		StatsDataFieldNameReportTotal: float64(1),
	}

	createGockReq().
		SetMatcher(generateSingleMessageBodyMatcher(bodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 关闭前会提交数据
	cnStatistician.Close()

	waitingForGockDone(t)
}

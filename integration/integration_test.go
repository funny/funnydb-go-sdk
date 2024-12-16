package integration

import (
	"context"
	"fmt"
	sdk "git.sofunny.io/data-analysis/funnydb-go-sdk"
	"git.sofunny.io/data-analysis/funnydb-go-sdk/internal"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

var userLoginEventName = "UserLogin"
var userLoginEvent = &sdk.Event{
	Time: time.Now(),
	Name: userLoginEventName,
	Props: map[string]interface{}{
		"#account_id": "account-fake955582",
		"#channel":    "tapdb",
		"#ip":         "123.23.11.155",
	},
}

func createClient(tmpDir string) (*sdk.Client, error) {
	config := &sdk.Config{
		Mode:           sdk.ModeAsync,
		IngestEndpoint: internal.IngestMockEndpoint, // 该地址不会创建统计上报影响测试结果
		SendTimeout:    5 * time.Second,
		AccessKey:      "demo",
		AccessSecret:   "demo",
		Directory:      tmpDir,
	}
	return sdk.NewClient(config)
}

func createClientWithStatistician(tmpDir string, sendInterval time.Duration) (*sdk.Client, error) {
	config := &sdk.Config{
		Mode:           sdk.ModeAsync,
		IngestEndpoint: internal.IngestCnEndpoint,
		SendTimeout:    5 * time.Second,
		SendInterval:   sendInterval,
		AccessKey:      "demo",
		AccessSecret:   "demo",
		Directory:      tmpDir,
	}
	return sdk.NewClient(config)
}

// 基础使用测试
func TestAsyncClient(t *testing.T) {
	tmpDir := fmt.Sprintf("/tmp/client-async-test-%d", time.Now().UnixNano())
	defer os.RemoveAll(tmpDir)

	c, err := createClient(tmpDir)
	assert.Nil(t, err)

	internal.CreateMockCollectGockReq().
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	internal.WaitingForGockDone(t)

	c.Close(context.Background())
}

// 测试正常重启后数据不会重复发送
func TestAsyncClientNormalRestart(t *testing.T) {
	tmpDir := fmt.Sprintf("/tmp/client-async-test-%d", time.Now().UnixNano())
	defer os.RemoveAll(tmpDir)

	c, err := createClient(tmpDir)
	assert.Nil(t, err)

	internal.CreateMockCollectGockReq().
		SetMatcher(internal.OneMessageSizeMatcher).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	internal.WaitingForGockDone(t)

	c.Close(context.Background())

	internal.CreateMockCollectGockReq().
		SetMatcher(internal.OneMessageSizeMatcher).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	c, err = createClient(tmpDir)
	assert.Nil(t, err)

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	internal.WaitingForGockDone(t)

	c.Close(context.Background())
}

// 测试数据异常丢弃该批次数据
func TestAsyncClientAuthErrorRestart(t *testing.T) {
	tmpDir := fmt.Sprintf("/tmp/client-async-test-%d", time.Now().UnixNano())
	defer os.RemoveAll(tmpDir)

	c, err := createClient(tmpDir)
	assert.Nil(t, err)

	internal.CreateMockCollectGockReq().
		SetMatcher(internal.OneMessageSizeMatcher).
		Times(1).
		Reply(400).
		JSON(map[string]interface{}{"error": "Bad Request"})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	internal.WaitingForGockDone(t)

	internal.CreateMockCollectGockReq().
		SetMatcher(internal.OneMessageSizeMatcher).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	internal.WaitingForGockDone(t)

	c.Close(context.Background())
}

// 测试 ingest 服务异常，过一段时间恢复后数据正确发送
func TestAsyncClientServerErrorRestart(t *testing.T) {
	tmpDir := fmt.Sprintf("/tmp/client-async-test-%d", time.Now().UnixNano())
	defer os.RemoveAll(tmpDir)

	c, err := createClient(tmpDir)
	assert.Nil(t, err)

	internal.CreateMockCollectGockReq().
		SetMatcher(internal.OneMessageSizeMatcher).
		Times(12).
		Reply(500).
		JSON(map[string]interface{}{"error": "ServerInternalError"})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	internal.WaitingForGockDone(t)

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	internal.CreateMockCollectGockReq().
		SetMatcher(internal.OneMessageSizeMatcher).
		Times(2).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	internal.WaitingForGockDone(t)

	c.Close(context.Background())
}

// 测试正确发送业务数据和统计数据
func TestAsyncClientStatistician(t *testing.T) {
	now := time.Now()
	statisticalBeginTime := now.Truncate(sdk.DefaultStatisticalInterval)
	statisticalEndTime := statisticalBeginTime.Add(sdk.DefaultStatisticalInterval)

	tmpDir := fmt.Sprintf("/tmp/client-async-test-%d", time.Now().UnixNano())
	defer os.RemoveAll(tmpDir)

	sendInterval := 1 * time.Second
	c, err := createClientWithStatistician(tmpDir, sendInterval)
	assert.Nil(t, err)

	eventBodyMap := map[string]interface{}{
		internal.DataFieldNameSdkType:    internal.SdkType,
		internal.DataFieldNameSdkVersion: internal.SdkVersion,
		internal.DataFieldNameEvent:      userLoginEventName,
	}

	internal.CreateCnCollectGockReq().
		SetMatcher(internal.GenerateMessageDataCheckMatcher(eventBodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	// 等待业务数据发送
	time.Sleep(3 * sendInterval)
	internal.WaitingForGockDone(t)

	statsBodyMap := map[string]interface{}{
		internal.StatsDataFieldNameBeginTime:   float64(statisticalBeginTime.UnixMilli()),
		internal.StatsDataFieldNameEndTime:     float64(statisticalEndTime.UnixMilli()),
		internal.StatsDataFieldNameEvent:       userLoginEventName,
		internal.StatsDataFieldNameReportTotal: float64(1),
	}

	internal.CreateCnCollectGockReq().
		SetMatcher(internal.GenerateMessageDataCheckMatcher(statsBodyMap)).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	// 关闭会触发发送统计数据
	err = c.Close(context.Background())
	assert.Nil(t, err)

	internal.WaitingForGockDone(t)
}

// 测试 ingest 服务异常过程中程序正常关闭
func TestAsyncClientServerErrorStop(t *testing.T) {
	tmpDir := fmt.Sprintf("/tmp/client-async-test-%d", time.Now().UnixNano())
	defer os.RemoveAll(tmpDir)

	c, err := createClient(tmpDir)
	assert.Nil(t, err)

	internal.CreateMockCollectGockReq().
		SetMatcher(internal.OneMessageSizeMatcher).
		Times(100).
		Reply(500).
		JSON(map[string]interface{}{"error": "ServerInternalError"})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	// 等待业务数据发送
	time.Sleep(10 * time.Second)

	c.Close(context.Background())
}

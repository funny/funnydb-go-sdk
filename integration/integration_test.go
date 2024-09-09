package integration

import (
	"context"
	"fmt"
	sdk "git.sofunny.io/data-analysis/funnydb-go-sdk"
	"git.sofunny.io/data-analysis/funnydb-go-sdk/internal"
	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
	"github.com/h2non/gock"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

var userLoginEvent = &sdk.Event{
	Time: time.Now(),
	Name: "UserLogin",
	Props: map[string]interface{}{
		"#account_id": "account-fake955582",
		"#channel":    "tapdb",
		"#ip":         "123.23.11.155",
	},
}

var singleMessageMatcher = gock.NewBasicMatcher()
var doubleMessageMatcher = gock.NewBasicMatcher()

func init() {
	singleMessageMatcher.Add(func(req *http.Request, ereq *gock.Request) (bool, error) {
		return checkRequestBody(req, 1)
	})
	doubleMessageMatcher.Add(func(req *http.Request, ereq *gock.Request) (bool, error) {
		return checkRequestBody(req, 2)
	})
}

func checkRequestBody(req *http.Request, msgSize int) (bool, error) {
	bytes, matcherErr := io.ReadAll(req.Body)
	if matcherErr != nil {
		return false, matcherErr
	}

	gunzipData, unzipErr := internal.GunzipData(bytes)
	if unzipErr != nil {
		return false, unzipErr
	}

	var batch client.Messages
	unmarshalErr := jsoniter.Unmarshal(gunzipData, &batch)
	if unmarshalErr != nil {
		return false, unmarshalErr
	}

	if len(batch.Messages) == msgSize {
		return true, nil
	}

	return false, nil
}

func createGockReq() *gock.Request {
	return gock.New("http://ingest.com").
		Post("/v1/collect")
}

func createClient(tmpDir string) (*sdk.Client, error) {
	config := &sdk.Config{
		Mode:           sdk.ModeAsync,
		IngestEndpoint: "http://ingest.com",
		SendTimeout:    5 * time.Second,
		AccessKey:      "demo",
		AccessSecret:   "demo",
		Directory:      tmpDir,
	}
	return sdk.NewClient(config)
}

func waitingForResponse() {
	for {
		if gock.IsDone() {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// 基础使用测试
func TestAsyncClient(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("client-async-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	c, err := createClient(tmpDir)
	assert.Nil(t, err)

	createGockReq().
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	waitingForResponse()

	c.Close(context.Background())
}

// 测试正常重启后数据不会重复发送
func TestAsyncClientNormalRestart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("client-async-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	c, err := createClient(tmpDir)
	assert.Nil(t, err)

	createGockReq().
		SetMatcher(singleMessageMatcher).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	waitingForResponse()

	c.Close(context.Background())

	createGockReq().
		SetMatcher(singleMessageMatcher).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	c, err = createClient(tmpDir)
	assert.Nil(t, err)

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	waitingForResponse()

	c.Close(context.Background())
}

// 测试异常重启后数据正确发送
func TestAsyncClientAuthErrorRestart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("client-async-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	c, err := createClient(tmpDir)
	assert.Nil(t, err)

	createGockReq().
		SetMatcher(singleMessageMatcher).
		Times(1).
		Reply(401).
		JSON(map[string]interface{}{"error": "Unauthorized"})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	waitingForResponse()

	// 等待 client 关闭
	time.Sleep(3 * time.Second)

	err = c.Close(context.Background())
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Unauthorized"))

	createGockReq().
		SetMatcher(doubleMessageMatcher).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	c, err = createClient(tmpDir)
	assert.Nil(t, err)

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	waitingForResponse()

	c.Close(context.Background())
}

// 测试异常重启后数据正确发送
func TestAsyncClientServerErrorRestart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("client-async-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	c, err := createClient(tmpDir)
	assert.Nil(t, err)

	createGockReq().
		SetMatcher(singleMessageMatcher).
		Times(5).
		Reply(500).
		JSON(map[string]interface{}{"error": "ServerInternalError"})

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	waitingForResponse()

	// 等待 client 关闭
	time.Sleep(3 * time.Second)

	err = c.Close(context.Background())
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "context deadline exceeded"))

	createGockReq().
		SetMatcher(doubleMessageMatcher).
		Times(1).
		Reply(200).
		JSON(map[string]interface{}{"error": nil})

	c, err = createClient(tmpDir)
	assert.Nil(t, err)

	err = c.ReportEvent(context.Background(), userLoginEvent)
	assert.Nil(t, err)

	waitingForResponse()

	c.Close(context.Background())
}

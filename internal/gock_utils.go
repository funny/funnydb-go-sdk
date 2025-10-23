package internal

import (
	"io"
	"net/http"
	"testing"
	"time"

	client "github.com/funny/ingest-client-go-sdk/v2"
	"github.com/h2non/gock"
	jsoniter "github.com/json-iterator/go"
)

var (
	OneMessageSizeMatcher = gock.NewBasicMatcher()
	TwoMessageSizeMatcher = gock.NewBasicMatcher()
)

func init() {
	OneMessageSizeMatcher.Add(func(req *http.Request, ereq *gock.Request) (bool, error) {
		return checkRequestBodyMsgSize(req, 1)
	})
	TwoMessageSizeMatcher.Add(func(req *http.Request, ereq *gock.Request) (bool, error) {
		return checkRequestBodyMsgSize(req, 2)
	})
}

func WaitingForGockDone(t *testing.T) {
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

func CreateGockReq(domain string, uri string) *gock.Request {
	return gock.New(domain).
		Post(uri)
}

func checkRequestBodyMsgSize(req *http.Request, msgSize int) (bool, error) {
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

	if len(batch.Messages) == msgSize {
		return true, nil
	}

	return false, nil
}

func GenerateMessageDataCheckMatcher(m map[string]interface{}) *gock.MockMatcher {
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

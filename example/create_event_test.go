package example

import (
	sdk "git.sofunny.io/data-analysis/funnydb-go-sdk"
	"testing"
	"time"
)

func TestCreateEvent(t *testing.T) {
	propsMap := map[string]interface{}{
		"#account_id": "account-fake955582",
		"#channel":    "tapdb",
		"other":       "test",
	}

	event := sdk.Event{
		Name:      "UserLogin",
		EventTime: time.Now(),
		Props:     propsMap,
	}
	t.Log(event)

	// 自动填充当前时间为事件时间
	sdk.NewEvent("UserLogin", propsMap)
}

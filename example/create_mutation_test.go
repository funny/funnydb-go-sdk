package example

import (
	sdk "git.sofunny.io/data-analysis/funnydb-go-sdk"
	"testing"
	"time"
)

func TestCreateMutation(t *testing.T) {
	propsMap := map[string]interface{}{
		"#account_id": "account-fake955582",
		"#channel":    "tapdb",
		"other":       "test",
	}

	mutation := sdk.Mutation{
		Type:      sdk.MutationTypeUser,
		Operate:   sdk.OperateTypeSet,
		Identity:  "user-id-1",
		EventTime: time.Now(),
		Props:     propsMap,
	}
	t.Log(mutation)

	// 自动填充当前时间为事件时间
	sdk.NewUserSetMutation("user-id-1", propsMap)
}

package example

import (
	"context"
	"fmt"
	sdk "git.sofunny.io/data-analysis/funnydb-go-sdk"
	"testing"
	"time"
)

func TestIngest(t *testing.T) {
	var ingestMaxBufferSize int = 5

	config := sdk.NewAnalyticsConfig()
	config.IngestMaxBufferSize = ingestMaxBufferSize
	analytics, err := sdk.NewFunnyDBAnalytics(sdk.ConsumerTypeIngest, config)
	if err != nil {
		t.Fatal(err)
	}

	propsMap := map[string]interface{}{
		"#account_id": "account-fake955582",
		"#channel":    "tapdb",
		"other":       "test",
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	// 这批数据达到最大缓存数量，会直接触发一次发送
	for i := 0; i < ingestMaxBufferSize; i++ {
		mutation := sdk.NewUserSetOnceMutation(fmt.Sprintf("user-id-%d", i), propsMap)
		err = analytics.Report(ctx, &mutation)
		if err != nil {
			t.Fatal(err)
		}
	}

	event := sdk.NewEvent("UserLogin", propsMap)

	err = analytics.Report(ctx, &event)
	if err != nil {
		t.Fatal(err)
	}

	// 由于单条数据达不到发送条件，close 后应该要把缓存的数据也发送完成
	err = analytics.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

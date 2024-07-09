package example

import (
	"context"
	sdk "git.sofunny.io/data-analysis/funnydb-go-sdk"
	"testing"
	"time"
)

func TestConsole(t *testing.T) {
	config := sdk.NewAnalyticsConfig()
	analytics, err := sdk.NewFunnyDBAnalytics(sdk.ConsumerTypeConsole, config)
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

	event := sdk.NewEvent("UserLogin", propsMap)

	err = analytics.Report(ctx, &event)
	if err != nil {
		t.Fatal(err)
	}

	mutation := sdk.NewUserSetOnceMutation("user1", propsMap)

	err = analytics.Report(ctx, &mutation)
	if err != nil {
		t.Fatal(err)
	}

	err = analytics.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

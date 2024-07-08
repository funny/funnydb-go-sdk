package example

import (
	sdk "git.sofunny.io/data-analysis/funnydb-go-sdk"
	"testing"
)

func TestConsole(t *testing.T) {
	config := sdk.NewAnalyticsConfig()
	analytics, err := sdk.NewFunnyDBAnalytics(sdk.ConsumerTypeConsole, config)
	if err != nil {
		t.Fatal(err)
	}

	event := sdk.NewEvent("UserLogin", map[string]interface{}{
		"#account_id": "account-fake955582",
		"#channel":    "tapdb",
		"other":       "test",
	})

	err = analytics.Report(&event)
	if err != nil {
		t.Fatal(err)
	}

	mutation := sdk.NewUserSetOnceMutation("user1", map[string]interface{}{
		"#account_id": "account-fake955582",
		"#channel":    "tapdb",
		"other":       "test",
	})

	err = analytics.Report(&mutation)
	if err != nil {
		t.Fatal(err)
	}

	err = analytics.Close()
	if err != nil {
		t.Fatal(err)
	}
}

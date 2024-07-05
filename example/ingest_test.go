package example

import (
	sdk "git.sofunny.io/data-analysis/funnydb-go-sdk/src"
	"git.sofunny.io/data-analysis/funnydb-go-sdk/src/consumer"
	"testing"
)

func TestIngest(t *testing.T) {
	analytics, err := sdk.NewIngestAnalytics(&consumer.IngestConsumerConfig{
		// 参数有默认值会自动注入
	})
	if err != nil {
		t.Fatal(err)
	}

	event := consumer.NewEvent("UserLogin", map[string]interface{}{
		"#account_id": "account-fake955582",
		"#channel":    "tapdb",
		"other":       "test",
	})

	err = analytics.Report(&event)
	if err != nil {
		t.Fatal(err)
	}

	mutation := consumer.NewUserSetOnceMutation("user1", map[string]interface{}{
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

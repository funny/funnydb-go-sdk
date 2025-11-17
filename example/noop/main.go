package main

import (
	"context"
	"log"
	"time"

	sdk "github.com/funny/funnydb-go-sdk/v2"
)

func main() {
	mode := sdk.ModeNoop

	config := &sdk.Config{
		Mode: mode,
	}

	client, err := sdk.NewClient(config)
	if err != nil {
		log.Fatal("创建 client 失败", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	event := sdk.Event{
		Time: time.Now(),
		Name: "UserLogin",
		Props: map[string]interface{}{
			"#account_id": "account-fake955582",
			"#channel":    "tapdb",
			"#ip":         "123.23.11.155",
		},
	}

	err = client.ReportEvent(ctx, &event)
	if err != nil {
		log.Fatal("发送 event 事件失败", err)
	}

	err = client.Close(ctx)
	if err != nil {
		log.Fatal("关闭 client 失败", err)
	}
}

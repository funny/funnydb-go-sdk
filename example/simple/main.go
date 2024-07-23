package main

import (
	"context"
	"flag"
	sdk "git.sofunny.io/data-analysis/funnydb-go-sdk"
	"log"
	"time"
)

func main() {
	endpoint := flag.String("endpoint", "http://localhost:8080", "ingest server endpoint")
	key := flag.String("key", "demo", "ingest server access key")
	secret := flag.String("secret", "secret", "ingest server access secret")
	flag.Parse()

	mode := sdk.ModeSimple

	config := &sdk.Config{
		Mode:           mode,
		IngestEndpoint: *endpoint,
		AccessKey:      *key,
		AccessSecret:   *secret,
	}

	client, err := sdk.NewClient(config)
	if err != nil {
		log.Fatal("创建 client 失败", err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	mutationPropsMap := map[string]interface{}{
		"#cpu_core_count": 6,
		"#screen_width":   1536,
		"#device_model":   "iPad11,1",
		"#screen_height":  2048,
		"#device_id":      "1af423ac5bcb9c657c0cecc4e5b354c5",
		"#cpu_model":      "",
		"#ram_capacity":   3,
		"#cpu_frequency":  0,
		"#sdk_version":    "0.9.3",
		"#os_platform":    "iPadOS",
		"#manufacturer":   "Apple",
		"#sdk_type":       "iOS",
	}

	mutation := sdk.Mutation{
		Time:     time.Now(),
		Type:     sdk.MutationTypeUser,
		Operate:  sdk.OperateTypeSet,
		Identity: "user-id-1",
		Props:    mutationPropsMap,
	}

	err = client.ReportMutation(ctx, &mutation)
	if err != nil {
		log.Fatal("发送 mutation 事件失败", err)
	}

	eventPropsMap := map[string]interface{}{
		"#account_id": "account-fake955582",
		"#channel":    "tapdb",
		"#ip":         "123.23.11.155",
	}

	event := sdk.Event{
		Time:  time.Now(),
		Name:  "UserLogin",
		Props: eventPropsMap,
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

package main

import (
	"context"
	"flag"
	"log"
	"time"

	sdk "github.com/funny/funnydb-go-sdk/v2"
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

	for i := 0; i < 10; i++ {
		eventPropsMap := map[string]interface{}{
			"#account_id": "account-fake955582",
			"#channel":    "tapdb",
			"#ip":         "123.23.11.155",
		}
		if i == 0 {
			log.Println("发送异常事件")
			eventPropsMap["answer"] = getAnswer
		}

		event := sdk.Event{
			Time:  time.Now(),
			Name:  "UserLogin",
			Props: eventPropsMap,
		}

		err = client.ReportEvent(ctx, &event)
		if err != nil {
			log.Println("发送 event 事件失败", err)
		} else {
			log.Println("发送成功")
		}
		time.Sleep(100 * time.Millisecond)
	}

	err = client.Close(ctx)
	if err != nil {
		log.Fatal("关闭 client 失败", err)
	}
}

func getAnswer() int64 {
	return 42
}

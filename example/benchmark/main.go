package main

import (
	"context"
	"flag"
	"log"
	"path/filepath"
	"time"

	sdk "github.com/funny/funnydb-go-sdk/v2"
)

func main() {
	endpoint := flag.String("endpoint", "http://localhost:7000", "ingest server endpoint")
	key := flag.String("key", "demo", "ingest server access key")
	secret := flag.String("secret", "secret", "ingest server access secret")
	directory := flag.String("directory", "./funnydb-go-sdk-benchmark", "log dir")
	testDuration := flag.Duration("duration", 30*time.Second, "test duration")
	modeText := flag.String("mode", "async", "mode")

	flag.Parse()

	mode := sdk.ModeAsync
	switch *modeText {
	case "async":
		mode = sdk.ModeAsync
	case "simple":
		mode = sdk.ModeSimple
	default:
		log.Fatal("unknown mode")
	}

	log.Printf("start mode: %s", *modeText)
	log.Printf("test duration: %s", *testDuration)

	config := &sdk.Config{
		Mode:           mode,
		IngestEndpoint: *endpoint,
		AccessKey:      *key,
		AccessSecret:   *secret,
		Directory:      filepath.Join(*directory, "data"),
	}

	client, err := sdk.NewClient(config)
	if err != nil {
		log.Fatal("创建 client 失败", err)
	}

	ctx := context.Background()

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

	deadline := time.Now().Add(*testDuration)

	sent := 0
	start := time.Now()
	maxCallTime := time.Duration(0)

	for time.Now().Before(deadline) {
		mutation := sdk.Mutation{
			Time:     time.Now(),
			Type:     sdk.MutationTypeUser,
			Operate:  sdk.OperateTypeSet,
			Identity: "user-id-1",
			Props:    mutationPropsMap,
		}

		callStart := time.Now()
		err = client.ReportMutation(ctx, &mutation)
		if err != nil {
			log.Fatal("发送 mutation 事件失败", err)
		}
		callElapsed := time.Since(callStart)
		if callElapsed > maxCallTime {
			maxCallTime = callElapsed
		}
		sent += 1
	}
	elapsed := time.Since(start)

	err = client.Close(ctx)
	if err != nil {
		log.Fatal("关闭 client 失败", err)
	}

	log.Printf("produces msgs: %d, avg_msgs_per_sec: %02f", sent, float64(sent)/elapsed.Seconds())
	log.Printf("max call duration: %s", maxCallTime.Round(time.Millisecond))
	log.Printf("avg call duration: %s", (elapsed / time.Duration(sent)).Round(time.Microsecond))
}

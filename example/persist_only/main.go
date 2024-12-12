package main

import (
	"context"
	"flag"
	"log"
	"time"

	sdk "github.com/funny/funnydb-go-sdk/v2"
)

func main() {
	directory := flag.String("directory", "./example-log-dir", "log dir")
	fileSize := flag.Int64("fileSize", 128, "max log file size(MB)")
	flag.Parse()

	mode := sdk.ModePersistOnly

	config := &sdk.Config{
		Mode:      mode,
		Directory: *directory,
		FileSize:  *fileSize,
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

	err = client.Close(ctx)
	if err != nil {
		log.Fatal("关闭 client 失败", err)
	}
}

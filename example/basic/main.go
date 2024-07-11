package main

import (
	"context"
	"fmt"
	sdk "git.sofunny.io/data-analysis/funnydb-go-sdk"
	"log"
	"time"
)

func main() {
	var maxBufferSize int = 5

	// 创建一份默认配置
	config := sdk.NewDefaultClientConfig()

	// 修改需要额外配置的属性
	config.ProducerMode = sdk.ProducerModeIngest
	config.MaxBufferSize = maxBufferSize

	client, err := sdk.NewClient(config)
	if err != nil {
		log.Fatal("创建 client 失败", err)
	}

	propsMap := map[string]interface{}{
		"#account_id": "account-fake955582",
		"#channel":    "tapdb",
		"other":       "test",
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	// 此前设置的 maxBufferSize = 5，这批数据达到最大缓存数量，会直接触发一次发送
	for i := 0; i < maxBufferSize; i++ {
		mutation := sdk.Mutation{
			EventTime: time.Now(),
			Type:      sdk.MutationTypeUser,
			Operate:   sdk.OperateTypeSet,
			Identity:  fmt.Sprintf("user-id-%d", i),
			Props:     propsMap,
		}

		err = client.ReportMutation(ctx, &mutation)
		if err != nil {
			log.Fatal("发送 mutation 事件失败", err)
		}
	}

	event := sdk.Event{
		EventTime: time.Now(),
		Name:      "UserLogin",
		Props:     propsMap,
	}

	err = client.ReportTrace(ctx, &event)
	if err != nil {
		log.Fatal("发送 trace 事件失败", err)
	}

	// 由于单条数据达不到发送条件，close 后应该要把缓存的数据也发送完成
	err = client.Close(ctx)
	if err != nil {
		log.Fatal("关闭 client 失败", err)
	}
}

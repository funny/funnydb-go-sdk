funnydb-go-sdk
---

## 获取方式

可以通过 go get 的方式来直接获取

```bash
env GOPRIVATE=git.sofunny.io \
	go get git.sofunny.io/data-analysis/funnydb-go-sdk@latest
```

## 用法

请参考 [example](example) 文件夹

## 产出数据样例
Event 事件
```json
{
    "data": {
      "#account_id": "account-fake955582",
      "#channel": "tapdb",
      "#event": "UserLogin",
      "#log_id": "01909fc7-548c-74f5-9d43-18b865b7362b",
      "#sdk_type": "go-sdk",
      "#sdk_version": "1.0.0",
      "#time": 1720667558739,
      "other": "test"
    },
    "type": "Event"
}
```

Mutation 事件
```json
{
    "data": {
      "#identify": "user-id-3",
      "#log_id": "01909fc7-5353-7bdf-b814-18c15a3b576c",
      "#operate": "set",
      "#sdk_type": "go-sdk",
      "#sdk_version": "1.0.0",
      "#time": 1720667558739,
      "properties": {
        "#account_id": "account-fake955582",
        "#channel": "tapdb",
        "other": "test"
      }
    },
    "type": "UserMutation"
}
```
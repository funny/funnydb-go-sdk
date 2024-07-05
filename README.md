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

## 特性
- 支持序列化，目前支持 JSON 和 msgpack
- 支持压缩操作，目前支持 gzip。压缩支持配置是否开启压缩操作，true 表示不开启，false 表示要开启
- 支持重试机制，当 ingest 那边返回的错误类型为 502、503、504 以及相关网络错误的时候，或者返回的错误类型为 102 （服务器已收到请求并正在处理，可重试），会进行相应的重试操作
- RetryTimeIntervalInitial：配置重试的间隔时间
- RetryTimeIntervalMax：重试时最大的间隔时间
- 重试的总时间会根据传入 Collect 中的 context 的生命周期来控制
- Logger 日志模块
- 支持自动生成 batchID 功能
- 自动并发分批发送

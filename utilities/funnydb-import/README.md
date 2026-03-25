oss-import
---

用于将离线 OSS/HTTP JSONL 数据导入 FunnyDB 的工具。

## 用法

```bash
go run . \
  -url-list ./urls.txt \
  -config ./config.yaml \
  -work-dir ./work
```

## URL 列表格式

```text
https://example-bucket/2026-03-24/file-0001.jsonl
https://example-bucket/2026-03-24/file-0002.jsonl
```

空行和以 `#` 开头的行会被忽略。

## 配置

参见 `config.example.yaml`。

## 行为说明

- 先将每个 URL 对应的文件下载到本地磁盘
- 将每条 JSONL 记录转换为 FunnyDB 的 `Event`
- 使用 `github.com/funny/ingest-client-go-sdk/v2` 按批发送
- 每成功发送一批，就持久化一次该 URL 的进度
- 支持断点续传
- 周期性输出进度日志
- 支持热更新以下配置项：
  - `import.batch_size`
  - `import.rate_limit.records_per_sec`

## 进度文件

进度文件会写入 `work/progress/` 目录，命名格式如下：

```text
<basename>.<url-sha1-8>.progress.json
```

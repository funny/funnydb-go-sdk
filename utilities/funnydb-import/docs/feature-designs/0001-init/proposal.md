# Proposal: OSS JSONL 导入 FunnyDB 工具

## 1. 背景

当前需求是实现一个离线导入程序：

1. 从一组 URL 下载数据文件；
2. 对 JSONL 数据逐条做固定预处理；
3. 按批发送到 FunnyDB；
4. 支持动态调整发送速率；
5. 支持断点续传；
6. 每隔 15 秒输出一次进度日志。

根据现有想法，发送链路使用 `github.com/funny/ingest-client-go-sdk/v2`，并参考其 `cmd/ingest-import` 实现。SDK 当前提供：

- `client.NewClient(config)`
- `client.Collect(ctx, &client.Messages{...})`
- 内建重试（部分 4xx/5xx/网络错误）
- 批量发送能力

因此本方案建议在 SDK 之上实现一个“下载 + 转换 + 批量导入 + 持久化进度”的命令行工具。

---

## 2. 目标

### 2.1 功能目标

- 输入 URL 列表文件，按顺序处理每个 URL；
- 每个 URL 对应一个进度文件；
- 如果进度未完成，可从断点恢复；
- 输入文件格式为 JSONL，每行一个 JSON 对象；
- 每条数据转换为：

```jq
. += {"#sdk_type": "go-sdk", "#zone_offset": 8} | {"type": "Event", "data": .}
```

- 按配置的批大小发送到 FunnyDB；
- 支持动态调整速率；
- 完成后标记该文件已完成，并删除本地数据文件；
- 每 15 秒打印一次整体和当前文件进度。

### 2.2 非目标

- 不做分布式多实例协同处理；
- 不做 UI，只提供 CLI；
- 不处理非 JSONL 格式；
- 第一版不强依赖远端文件随机读能力（即不要求 HTTP Range 精准断点下载）。

---

## 3. 总体设计

程序建议命名为：`oss-importer`（名称可调整）。

整体流程：

```text
URL 列表文件
   -> 逐个 URL 处理
      -> 读取/创建进度文件
      -> 若本地数据文件不存在则下载
      -> 从断点位置读取 JSONL
      -> 逐条预处理
      -> 按批发送到 FunnyDB
      -> 每成功发送一批后落盘进度
      -> 文件完成后标记 done=true
      -> 删除本地数据文件
      -> 继续下一个 URL
```

考虑到这是第一版工具，建议**全部实现先放在 `main.go`**，保持结构简单、便于快速交付。

代码层面不强行拆 package，但仍按逻辑分成几组函数：

1. 参数解析与配置加载
2. URL 列表读取
3. 进度文件读写与原子落盘
4. 文件下载
5. JSONL 逐行转换
6. 批量发送到 FunnyDB
7. 周期性进度日志
8. 单文件处理主流程

如果后续功能继续增长，再把这些函数按职责拆到独立文件或 package。

---

## 4. 输入与输出

### 4.1 CLI 参数

建议提供以下参数：

```text
oss-importer \
  -url-list ./urls.txt \
  -config ./config.yaml \
  -work-dir ./work
```

### 4.2 参数说明

- `-url-list`
  - 必填
  - 文本文件，每行一个 URL
- `-config`
  - 必填
  - 配置文件，包含 FunnyDB 连接信息、速率、批大小等
- `-work-dir`
  - 可选，默认 `./work`
  - 用于保存下载文件、进度文件、临时文件
- `-resume`
  - 可选，默认 `true`
  - 是否开启断点续传
- `-progress-log-interval`
  - 可选，默认 `15s`

### 4.3 URL 列表文件格式

```text
https://example-bucket/2026-03-24/file-0001.jsonl
https://example-bucket/2026-03-24/file-0002.jsonl
```

忽略空行与 `#` 开头注释行。

---

## 5. 配置文件设计

建议使用 YAML，便于人工修改并支持热更新。

示例：

```yaml
ingest:
  endpoint: https://ingest.zh-cn.xmfunny.com
  access_key_id: xxx
  access_key_secret: yyy
  concurrency: 4
  request_timeout: 30s

import:
  batch_size: 1000
  rate_limit:
    records_per_sec: 5000

runtime:
  work_dir: ./work
  temp_dir: ./work/tmp
  data_dir: ./work/data
  progress_dir: ./work/progress
  config_reload_interval: 5s
  progress_log_interval: 15s
```

其中以下行为保持为程序内固定策略，不额外暴露配置：

- 每成功发送一批后立即落盘进度；
- 文件处理完成后删除本地数据文件。

### 5.1 动态配置项

以下字段建议支持运行时热更新：

- `import.batch_size`
- `import.rate_limit.records_per_sec`

实现方式建议为：

- 每 5 秒检查配置文件修改时间；
- 若文件变化则重新加载并校验；
- 通过原子变量或带锁配置对象替换当前运行配置；
- 已经构造完成的批次继续按旧配置发送；
- 新批次按新配置执行。

---

## 6. 工作目录与文件布局

建议统一放到 `work-dir` 下：

```text
work/
  data/
    file-0001.jsonl
  progress/
    file-0001.<sha1-8>.progress.json
  tmp/
    file-0001.jsonl.part
```

### 6.1 为什么进度文件加 hash

原始想法是“与文件同名”。如果仅使用 basename，可能发生冲突：

- 不同 URL 路径下文件同名；
- 带 query 参数的 URL 映射不稳定。

因此建议：

- 本地数据文件名：使用 URL basename；
- 进度文件名：`<basename>.<url-sha1-8>.progress.json`

同时在进度文件内容中保留完整 URL，便于追踪。

---

## 7. 进度文件设计

每个 URL 对应一个 JSON 进度文件。

示例：

```json
{
  "url": "https://example-bucket/2026-03-24/file-0001.jsonl",
  "file_name": "file-0001.jsonl",
  "local_data_path": "work/data/file-0001.jsonl",
  "download_completed": true,
  "download_size": 987654321,
  "processed_lines": 120000,
  "processed_bytes": 183456789,
  "sent_batches": 120,
  "sent_records": 120000,
  "done": false,
  "last_error": "",
  "updated_at": "2026-03-24T10:00:00Z"
}
```

### 7.1 核心字段说明

- `download_completed`
  - 文件是否已完整下载到本地
- `processed_lines`
  - 已成功发送到 FunnyDB 的行数
  - **断点恢复以此为主**
- `processed_bytes`
  - 已消费字节数，仅用于日志和辅助定位
- `sent_batches`
  - 已成功发送批次数
- `sent_records`
  - 已成功发送记录数
- `done`
  - 当前文件是否完整处理结束
- `last_error`
  - 最近一次错误，便于排查

### 7.2 为什么以行号作为恢复点

JSONL 是天然的逐行记录格式。若仅记录字节偏移：

- 需要保证恢复时偏移刚好落在换行边界；
- 还要考虑 UTF-8、多字节字符、不同解码器行为。

因此建议：

- 以 `processed_lines` 作为恢复基准；
- 恢复时重新打开文件并跳过前 N 行；
- 当前批次只有在 **整批成功发送后** 才更新进度。

这样可以保证“至少一次”语义下的一致性，并避免半批次状态复杂化。

### 7.3 进度落盘策略

每成功发送一批后：

1. 更新内存中的进度对象；
2. 写入 `*.tmp` 临时文件；
3. `rename` 原子替换正式进度文件。

这样即使进程异常退出，也不会生成半截 JSON 文件。

---

## 8. 下载策略

### 8.1 第一版建议：整文件下载后再导入

虽然需求中包含“读取，预处理，发送到 funnydb”，但结合断点续传要求，第一版建议采用：

1. 若本地无数据文件，则先完整下载到 `*.part`；
2. 下载完成后 rename 为正式数据文件；
3. 再开始逐行导入；
4. 导入完成后删除正式数据文件。

优点：

- 实现简单；
- 下载与导入的故障边界清晰；
- 恢复逻辑稳定；
- 不依赖服务器支持 Range；
- 避免“边下载边发送”导致下载位置与发送位置双重断点复杂化。

### 8.2 未来可扩展：流式下载导入

后续若数据量极大、磁盘受限，可支持：

- HTTP Range 续传下载；
- 边下载边解析边发送；
- 下载偏移与发送偏移双进度管理。

但不建议作为第一版范围。

---

## 9. 数据预处理设计

原始规则：

```jq
. += {"#sdk_type": "go-sdk", "#zone_offset": 8} | {"type": "Event", "data": .}
```

Go 中建议实现为：

1. 每行反序列化为 `map[string]any`；
2. 向 map 写入：
   - `#sdk_type = "go-sdk"`
   - `#zone_offset = 8`
3. 组装为：

```json
{
  "type": "Event",
  "data": { ... }
}
```

对应 SDK 结构：

```go
type Message struct {
    Type string
    Data interface{}
}
```

### 9.1 错误处理建议

单行 JSON 非法时需要明确策略。建议第一版提供两种模式：

- `strict`：遇到非法行立即失败；
- `skip_bad_line`：记录错误日志并跳过，统计坏行数。

默认建议 `strict`，避免静默丢数据。

---

## 10. 导入策略

### 10.1 与 SDK 对齐

参考 `cmd/ingest-import`，建议使用：

- `client.NewClient(config)`
- `client.Collect(ctx, &client.Messages{Messages: batch})`

每批大小由 `batch_size` 控制。

### 10.2 批处理模型

处理单个文件时：

1. 打开本地 JSONL 文件；
2. 跳过 `processed_lines`；
3. 逐行读取并转换；
4. 累积到 `batch_size`；
5. 到达批阈值后发送；
6. 成功后更新进度；
7. 循环直到 EOF；
8. 发送尾批；
9. 标记 `done=true`。

### 10.3 限速策略

需求提到“支持动态调整串流速率”。建议实现为**发送前限速**，按 `records/s` 控制。

第一版保持简单：

- 仅暴露 `rate_limit.records_per_sec`；
- 不额外暴露 `burst` 参数；
- 按“每批发送前等待本批所需额度”的方式限速。

这样可以兼顾：

- 实现简单；
- 性能损耗低；
- 与批发送模型匹配；
- 配置项少，运行时更容易调整。

### 10.4 并发策略

建议第一版默认：

- URL 级别串行处理；
- 单文件内部可并发发送批次，但要谨慎。

由于断点是“成功发送一批即推进”，如果批次并发发送，会出现：

- 批 3 先成功，批 2 后成功；
- 进度推进顺序需要额外保证。

因此建议第一版：

- **单文件按批串行发送**；
- 依赖 SDK 内部网络重试；
- 先保证恢复语义简单可靠。

后续如需提升吞吐，再引入“有序确认”的并发批发送。

---

## 11. 断点续传语义

### 11.1 语义定义

本方案提供的是：

- **文件级断点恢复**
- **批级提交**
- **至少一次（at-least-once）发送语义**

即：

- 某批发送成功后，进度才前移；
- 若发送成功但进度文件尚未来得及落盘，进程崩溃后该批可能被重复发送；
- 因此下游需能够接受少量重复，或由业务字段去重。

### 11.2 为什么不追求 exactly-once

要做到 exactly-once，需要满足至少一项：

- 下游提供幂等写入键；
- 客户端持久化更细粒度 ACK 状态；
- 服务端支持事务性提交。

当前需求与 SDK 信息不足以支撑这类设计，因此第一版建议明确采用 at-least-once。

---

## 12. 日志与可观测性

### 12.1 周期性进度日志

每 15 秒打印一次，建议字段：

- `current_url`
- `file_name`
- `download_completed`
- `processed_lines`
- `processed_bytes`
- `sent_batches`
- `sent_records`
- `current_rate_records_per_sec`
- `batch_size`
- `done`

示例：

```text
level=INFO msg="progress" file=file-0001.jsonl processed_lines=120000 sent_records=120000 sent_batches=120 batch_size=1000 rate_limit=5000 done=false
```

### 12.2 关键事件日志

建议记录：

- 开始处理某个 URL
- 下载开始/完成
- 读取到已有进度并恢复
- 每批发送成功
- 配置热更新生效
- 单文件完成
- 删除本地文件
- 错误与重试信息

---

## 13. 异常场景处理

### 13.1 下载失败

- 保留 `.part` 文件；
- 标记 `download_completed=false`；
- 程序退出非 0；
- 下次启动重新下载（第一版可直接覆盖旧 `.part`）。

### 13.2 发送失败

- 当前批失败则停止当前文件处理；
- 不推进进度；
- 错误写入 `last_error`；
- 程序退出非 0。

### 13.3 配置文件格式错误

- 热更新失败时保留旧配置继续运行；
- 打错误日志，不中断当前导入。

### 13.4 进度文件损坏

建议第一版直接失败并提示人工处理，避免误覆盖导致重复导入。

### 13.5 本地文件丢失但进度存在

- 若 `done=true`：视为已完成，跳过；
- 若 `done=false`：重新下载并从 `processed_lines` 恢复。

---

## 14. 安全与运维建议

- `access_key_secret` 不应出现在普通日志中；
- 配置文件权限建议限制；
- 支持从环境变量覆盖敏感配置（可选增强）；
- K8s 下建议挂载：
  - URL 列表文件
  - 配置文件
  - 持久化 work 目录（PVC）

如果 work 目录不持久化，Pod 重建后会丢失进度与本地文件，无法实现真正恢复。

---

## 15. 建议的数据结构

### 15.1 配置结构

```go
type Config struct {
    Ingest  IngestConfig  `yaml:"ingest"`
    Import  ImportConfig  `yaml:"import"`
    Runtime RuntimeConfig `yaml:"runtime"`
}
```

### 15.2 进度结构

```go
type FileProgress struct {
    URL               string    `json:"url"`
    FileName          string    `json:"file_name"`
    LocalDataPath     string    `json:"local_data_path"`
    DownloadCompleted bool      `json:"download_completed"`
    DownloadSize      int64     `json:"download_size"`
    ProcessedLines    int64     `json:"processed_lines"`
    ProcessedBytes    int64     `json:"processed_bytes"`
    SentBatches       int64     `json:"sent_batches"`
    SentRecords       int64     `json:"sent_records"`
    Done              bool      `json:"done"`
    LastError         string    `json:"last_error"`
    UpdatedAt         time.Time `json:"updated_at"`
}
```

---

## 16. 伪代码

```go
for _, url := range urls {
    progress := loadOrInitProgress(url)
    if progress.Done {
        continue
    }

    if !progress.DownloadCompleted || !fileExists(progress.LocalDataPath) {
        downloadToLocal(url, progress.LocalDataPath)
        progress.DownloadCompleted = true
        saveProgress(progress)
    }

    f := open(progress.LocalDataPath)
    skipLines(f, progress.ProcessedLines)

    batch := make([]client.Message, 0, cfg.Import.BatchSize)
    for each line in f {
        obj := parseJSON(line)
        msg := transform(obj)
        batch = append(batch, msg)

        if len(batch) == cfg.Import.BatchSize {
            limiter.WaitN(ctx, len(batch))
            sendBatch(batch)
            progress.ProcessedLines += int64(len(batch))
            progress.SentRecords += int64(len(batch))
            progress.SentBatches++
            saveProgress(progress)
            batch = batch[:0]
        }
    }

    if len(batch) > 0 {
        limiter.WaitN(ctx, len(batch))
        sendBatch(batch)
        progress.ProcessedLines += int64(len(batch))
        progress.SentRecords += int64(len(batch))
        progress.SentBatches++
        saveProgress(progress)
    }

    progress.Done = true
    saveProgress(progress)
    remove(progress.LocalDataPath)
}
```

---

## 17. 测试建议

### 17.1 单元测试

- 配置加载与热更新
- URL 到本地文件名映射
- 进度文件读写与原子替换
- JSONL 转换逻辑
- 跳过前 N 行恢复逻辑
- 限速器行为

### 17.2 集成测试

- 本地 HTTP server 模拟文件下载
- mock ingest server 校验批量请求内容
- 中途 kill 进程后重启，验证恢复
- 配置热修改后速率变化生效

### 17.3 故障测试

- 下载中断
- ingest 500/502/429
- 坏 JSON 行
- 进度文件损坏

---

## 18. 分阶段实施建议

### Phase 1：最小可用版本

- URL 列表读取
- 本地整文件下载
- JSONL 转换
- 批量发送
- 每批进度落盘
- 断点恢复
- 15 秒日志

### Phase 2：增强

- 配置热更新
- 限速器动态调整
- 坏行跳过模式
- 更细致指标

### Phase 3：性能优化

- 有序并发批发送
- Range 下载续传
- 流式下载 + 导入

---

## 19. 待确认问题

在正式实现前，建议确认以下事项：

1. URL 来源是否一定是 OSS/HTTP，是否需要鉴权下载？
2. 下游 FunnyDB 是否允许重复数据，是否有幂等键？
3. 速率控制希望按“记录数/秒”还是“字节数/秒”？
4. `batch_size` 动态修改时，是否允许仅对后续批次生效？
5. 本地 work 目录是否有持久化存储保障？
6. 非法 JSON 行是否必须失败，还是允许跳过？
7. 是否需要导入完成清理进度文件，还是长期保留审计记录？

---

## 20. 结论

建议第一版采用：

- **整文件下载后导入**
- **按行恢复**
- **按批提交并落盘进度**
- **单文件串行发送**
- **token bucket 动态限速**
- **配置文件热更新**
- **at-least-once 语义**

这个方案实现复杂度适中，能覆盖当前最核心需求：

- 可恢复
- 可控速
- 可观测
- 易于在 K8s 中稳定运行

如果需要，下一步我可以继续基于这个 proposal 拆出更具体的：

1. `main.go` 内部函数划分；
2. `config.yaml` 最终字段定义；
3. `progress.json` 最终 schema；
4. MVP 实现任务清单。
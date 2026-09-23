# Telemetry Aggregator（本机运行的遥测聚合与告警）

一个纯内存、单机运行的遥测聚合服务：并发摄取、按事件时间的窗口聚合、调用链还原与
防抖告警，对外提供一致的查询。基于 Spring Boot 3 / Java 21，无外部中间件依赖。

- **摄取、聚合、告警、查询共用同一个单工作线程串行上下文**，所有结论确定、可复现、可解释。
- 写入线程只做“校验 + 幂等判重 + 非阻塞入有界队列”，**高并发下不丢计数、不错乱、不长时间阻塞**。
- 迟到、重复、标识冲突、基数超限都以**可区分的拒绝原因**返回并计入 `/telemetry/stats`，绝不静默计入或静默丢弃。

---

## 1. 构建与启动

```bash
mvn package                # 构建并运行测试（16 个测试）
java -jar target/telemetry-aggregator-java-1.0-SNAPSHOT.jar
# 或开发模式
mvn spring-boot:run
```

服务默认监听 `18090`（本机 8080 常被占用，可用 `--server.port=` 覆盖）。所有配置项见 `src/main/resources/application.properties`，
可用 `--telemetry.xxx=` 或环境变量覆盖，与 Spring Boot 常规配置方式一致。

---

## 2. 窗口与水位线规则（确定且可复现）

### 2.1 窗口归属

固定长度滚动窗口（默认 1000ms）。事件时间 `t` 所属窗口：

```
windowStart = floor(t / windowMillis) * windowMillis     # Java floorDiv，负时间也确定
windowEnd   = windowStart + windowMillis
```

窗口在第一条属于它的事件到达时**惰性创建**；窗口内按 `metric + 规范化标签` 聚合成 series，
产出 `count / sum / min / max / avg`。

### 2.2 水位线推进

```
maxEventTime = max(已观察到的事件时间)          # 单调不减
watermark    = maxEventTime - allowedLatenessMillis   # 单调不减
```

- 初始（无事件、未手动推进）水位线为“负无穷”，不拒绝任何事件。
- 每处理一条事件先判迟到，再用该事件时间推进水位线，因此事件不会被自己推进的水位线拒绝。
- `windowEnd <= watermark` 的窗口按起点升序**关闭（finalize）**为不可变快照。
  注意是 `<=`：水位线恰好等于窗口上界时该窗口即关闭。

### 2.3 迟到与乱序

- **乱序（尚未越过水位线的晚到事件）是正常的**：事件通过摄取入口后进入唯一的 FIFO
  队列，工作线程严格按入队顺序处理；只要轮到某事件时它不晚于当前水位线，就计入其
  所属窗口（窗口在 `windowEnd <= watermark` 关闭前一直可写）。
- **迟到判定唯一发生在工作线程的 FIFO 处理点**：`eventTime < watermark`
  （**等于水位线仍接受**）。若 FIFO 内更晚提交的更大时间戳事件已把水位线推过该
  事件、其窗口已关闭，则以 `LATE_BEHIND_WATERMARK` 拒绝，响应与日志给出事件时间、
  水位线与“未计数”原因。摄取入口不判晚，避免事件与“推进水位线”并发时产生多余拒绝。
- 迟到事件**不会进入窗口，也不会因后续重试而补计**；拒绝结论被去重表记住，
  同一 `eventId` 无论重试多少次、由哪个线程重试，得到的都是同一条拒绝结论。
- 水位线推进/查询走优先控制队列，且控制任务执行前会先排空已入队的摄取事件，
  因此“推进水位线”绝不会越过其调用前已提交的事件（保证可复现的先后关系）。

### 2.4 本地演示如何关闭残余窗口

事件流停止后水位线不再自动前进，可手动前推（单调、不可后退）：

```bash
curl -X POST 'http://localhost:18090/telemetry/watermark/advance?eventTimeMillis=5000'
```

该请求排在所有已入队事件之后执行，返回推进后的 `/stats`。

---

## 3. 幂等、重复投递与标识冲突

`eventId` 是投递幂等键。事件负载（时间戳、metric、value、标签、trace/span 字段）
按 key 排序后做 SHA-256 指纹。

| 情况 | 结论 | 语义 |
|---|---|---|
| 首次投递 | `ACCEPTED` 或具体拒绝原因 | 正常处理 |
| 相同 `eventId` + **相同负载** | 回放到首次结论 | **永不重复计数**；重试拿到与首次逐字节一致的结论；`stats.dedupReplays` 累加 |
| 相同 `eventId` + **不同负载** | `ID_CONFLICT` | 标识互相冲突，显式拒绝，不覆盖任何已有数据 |
| `spanId` 同 trace 内重复但 parent/duration 等不同 | `ID_CONFLICT` | 已记录节点不被覆盖 |

实现要点：同 `eventId` 由 64 路条纹锁互斥；首次事件的结论 `CompletableFuture`
保存在去重表中，**在首次事件尚在队列中时到达的并发重复直接共享同一个 future**，
因此“首次入队失败回滚”与“重试判重”之间不存在竞争窗口。首次若因队列满未能入队，
其去重占位会被回滚，不毒化该 id（后续重试仍可正常接受）。

---

## 4. 并发模型、有界保护与背压

```
HTTP 线程:  校验 → 条纹锁判重 → 非阻塞 offer 到有界 eventQueue（满则立即 QUEUE_FULL）
                                      │
工作线程(单线程): 排空 eventQueue(FIFO) → 逐条: 迟到检查 → 链路 → 窗口基数校验/计数
                    → 推进水位线/关窗 → 告警评估 → 链路剪枝
控制队列(优先):  查询 / 水位线推进 / awaitDrained（每次先排空已排队事件再执行）
```

- **有界摄取队列** `telemetry.ingest-queue-capacity`（默认 10000）：队列满时
  `ingest` 立即以 `QUEUE_FULL` 拒绝并返回原因，调用方不需要阻塞等待。
- **去重表容量** `telemetry.dedup-capacity`：新 eventId 超出上限 → `DEDUP_CAPACITY`。
- **标签基数保护**：
  - 单窗口单个 tag key 的不同 value 数超过 `telemetry.max-tag-values-per-key`
    → `TAG_CARDINALITY`（原因注明是哪个 key）；
  - 单窗口 series 数超过 `telemetry.max-series-per-window` → `TAG_CARDINALITY`；
  - 所有基数检查在**任何状态修改之前**完成，拒绝不会产生部分写入。
- **活动窗口数**超过 `telemetry.max-active-windows` → `TOO_MANY_ACTIVE_WINDOWS`。
- **调用链 span 总数**超过 `telemetry.max-spans` → `TAG_CARDINALITY`（注明 span 上限）。
- 已关闭窗口与告警事件分别只保留最近 `retained-windows` / `retained-alert-events` 条，
  内存占用有上界。

当前堆积量与丢弃情况通过 `GET /telemetry/stats` 观察：

```jsonc
{
  "received": 1234,
  "dedupReplays": 56,            // 命中幂等回放的次数
  "byStatus": { "ACCEPTED": ..., "QUEUE_FULL": ..., "LATE_BEHIND_WATERMARK": ...,
                "ID_CONFLICT": ..., "TAG_CARDINALITY": ..., "...": 0 },
  "queueDepth": 0,               // 当前摄取队列堆积
  "dedupSize": 1200,             // 去重表当前条目
  "spanCount": 0,                // 当前调用链 span 总量
  "activeWindows": 1,            // 活动窗口数
  "closedWindows": 3,
  "watermark": 2500,
  "maxObservedEventTime": 3000
}
```

---

## 5. 调用链还原

携带 `traceId / spanId / parentSpanId` 的事件参与还原（`parentSpanId` 空表示根）。

- **乱序到达**：子 span 先于父 span 到达时，链路标记 `AWAITING_PARENT`，
  深度按“以缺失父的节点为临时根”计算；父 span 到达后自动升级为 `COMPLETE`。
  **链路永不丢弃，判定不中断。**
- **环路**：父引用出现自环或多节点环时标记 `CYCLE`，并给出环上 spanId 序列。
- **多根**：同一 trace 出现多个根 → `DISCONNECTED`。
- **剪枝**：水位线推进后，完整且整体已过窗的链路被删除（内存有界）；
  父 span 已不可能到达的孤立链改标 `DISCONNECTED` 并保留可查，而不是静默删除。

---

## 6. 告警稳定性规则（防抖动）

规则针对某条 trace 的链路指标（`CHAIN_DURATION` 总耗时 或 `CHAIN_DEPTH` 深度），
在链路每次更新时评估。两道防抖：

1. **连续计数**：观测值连续 `fireConsecutive` 次 `>= fireThreshold` 才进入
   `FIRING`；`FIRING` 后连续 `resolveConsecutive` 次 `<= resolveThreshold`
   才恢复 `OK`。计数期间任何一次不满足即清零——**单次尖峰/瞬时回落不会触发或恢复**。
2. **滞后带（hysteresis）**：`resolveThreshold < value < fireThreshold` 时，
   该值既不满足触发也不满足恢复，两个方向的连续计数都**冻结保持**（不归零），
   状态不变——带内的来回波动既不会触发/恢复，也不会打断带外连续计数的积累；
   只有“明确高”或“明确低”才会推进或重置对应计数。

状态机：`OK → PENDING_FIRING → FIRING → PENDING_RESOLVED → OK`。
`PENDING_*` 中间态可通过 `/telemetry/alerts/states` 观察；仅真正的 FIRING 与恢复
产生事件（`/telemetry/alerts/events`），事件携带观测值与判定依据文本。

孤立/乱序/成环链路同样参与告警评估（使用当前可还原出的指标值），
结构问题通过链路状态另行标记，二者互不阻断。

---

## 7. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/telemetry/events` | 摄取单条事件，返回 `IngestResult`（状态/原因/窗口/水位线） |
| POST | `/telemetry/events/batch` | 批量摄取，逐条返回结论（保持入队顺序） |
| GET | `/telemetry/windows?from=&to=` | 查询已关闭窗口（窗口起点区间，可选） |
| GET | `/telemetry/chains/{traceId}` | 查询单条调用链（状态/环/深度/总耗时/span 列表） |
| GET | `/telemetry/chains` | 查询全部调用链 |
| POST | `/telemetry/alerts/rules` | 注册告警规则 |
| GET | `/telemetry/alerts/events?limit=` | 最近的触发/恢复事件（最新在前） |
| GET | `/telemetry/alerts/states` | 各规则当前状态机状态与连续计数 |
| POST | `/telemetry/watermark/advance?eventTimeMillis=` | 单调前推水位线（本地演示/关窗） |
| GET | `/telemetry/stats` | 接收量、各类拒绝计数、堆积量与表规模、水位线 |

事件 JSON 示例：

```json
{
  "eventId": "e-1001",
  "timestampMillis": 1200,
  "metric": "cpu",
  "value": 42.5,
  "tags": { "host": "a", "region": "cn" },
  "traceId": "tr-1",
  "spanId": "child-1",
  "parentSpanId": "root",
  "durationMillis": 80
}
```

摄取结果示例（拒绝同样返回 200 + 明确状态，便于客户端区分处理）：

```json
{ "status": "LATE_BEHIND_WATERMARK",
  "reason": "eventTime 500 < watermark 2500; the containing window is already finalized, rejected (not counted)",
  "eventTimestamp": 500, "windowStart": 0, "watermark": 2500, "counted": false }
```

---

## 8. 本地验证

### 8.1 一键脚本

```bash
./verify-local.sh          # 启动服务 → 造数 → 校验各类结论 → 关闭
```

脚本覆盖：正常聚合、重复不双计、ID 冲突、非法事件、乱序补链、告警防抖触发、
水位线关窗、迟到拒绝与 stats 核对。

### 8.2 手工验证

```bash
# 摄取
curl -s -X POST localhost:18090/telemetry/events -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","timestampMillis":1200,"metric":"cpu","value":10,"tags":{"host":"a"}}'
# 推进水位线关闭窗口并查询
curl -s -X POST 'localhost:18090/telemetry/watermark/advance?eventTimeMillis=3000' >/dev/null
curl -s localhost:18090/telemetry/windows
# 观察计数、拒绝原因与堆积量
curl -s localhost:18090/telemetry/stats
```

### 8.3 单元/集成测试

```bash
mvn test
```

| 测试类 | 覆盖场景 |
|---|---|
| `WindowWatermarkTest` | floor 窗口归属、水位线单调、`end<=wm` 关窗边界、迟到边界 |
| `ChainTrackerTest` | 乱序补全、自环/多节点环、spanId 冲突、多根、剪枝标记 |
| `AlertEngineTest` | 连续计数、滞后带、单尖峰不触发、抖动不反复触发/恢复 |
| `AggregationEngineIntegrationTest` | 重复重试不双计、ID 冲突、迟到拒绝、16 线程并发去重一致性、有界队列背压、标签基数上限 |
| `AlertFlowEndToEndTest` | 乱序链路 + 告警防抖端到端、查询与摄取结论对齐 |

测试日志会打印**输入事件与判定依据**（窗口起点、水位线、状态机状态/连续计数、
拒绝原因），便于复核。

---

## 9. 配置项一览

| 配置 | 默认值 | 含义 |
|---|---|---|
| `telemetry.window-millis` | 1000 | 滚动窗口长度 |
| `telemetry.allowed-lateness-millis` | 500 | 水位线迟到容忍 |
| `telemetry.ingest-queue-capacity` | 10000 | 摄取有界队列容量（背压阈值） |
| `telemetry.dedup-capacity` | 1000000 | eventId 去重表容量 |
| `telemetry.max-tag-values-per-key` | 10000 | 单窗口单 tag key 不同 value 上限 |
| `telemetry.max-series-per-window` | 100000 | 单窗口 series 上限 |
| `telemetry.max-active-windows` | 1000 | 活动窗口数上限 |
| `telemetry.max-spans` | 500000 | 调用链 span 总数上限 |
| `telemetry.retained-windows` | 500 | 保留的已关闭窗口数 |
| `telemetry.retained-alert-events` | 1000 | 保留的告警事件数 |

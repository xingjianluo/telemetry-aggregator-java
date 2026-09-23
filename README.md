# Telemetry Aggregator（本机运行的遥测聚合与告警）

一个基于 Spring Boot 3 / Java 21 风格（release 17 编译）的单机遥测聚合服务：
高并发摄取、按**事件时间**做窗口聚合、依据父子 span 还原调用链并进行稳定告警，
对外提供摄取 / 聚合 / 告警 / 链路 / 指标五类 HTTP 接口。

摄取、聚合、告警、查询四部分语义对齐：

* 聚合只读取**定稿窗口**的数据，告警只由**定稿窗口**驱动，对外查询也只返回定稿窗口；
* 事件是否计入只由一条线性化管道决定，结论可解释、可复现、可重试；
* 任何拒绝都携带可区分的 `RejectReason`，不静默计入也不静默丢弃。

---

## 1. 构建与运行

```bash
# 编译 + 跑全部测试
mvn clean test

# 打包
mvn clean package -DskipTests

# 启动（沿用 Spring Boot 原生方式）
mvn spring-boot:run
# 或
java -jar target/telemetry-aggregator-java-1.0-SNAPSHOT.jar
```

服务默认监听 `8080`。所有参数集中在 `src/main/resources/application.properties`，
均可用环境变量 / 命令行覆盖（前缀 `telemetry.*`）。

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events` | 投递单条事件；202=接受，422=拒绝（响应体含原因与依据） |
| POST | `/events/batch` | 批量投递；逐条给出结论，任一拒绝整体返回 422 |
| GET | `/aggregates?service=&metric=` | 查询定稿窗口聚合 |
| GET | `/alerts` | 查询全部规则 × 对象的告警状态与判定依据 |
| GET | `/traces/{traceId}` | 查询调用链拓扑还原结果与状态原因 |
| GET | `/metrics` | 接受量、按原因区分的拒绝量、队列堆积、基数占用、水位线 |

事件字段：

```json
{
  "eventId": "evt-1",
  "eventTimeMillis": 1727000000123,
  "traceId": "trace-1",
  "spanId": "span-b",
  "parentSpanId": "span-a",
  "service": "svc-a",
  "metric": "cpu",
  "value": 42.0,
  "tags": { "host": "h1", "region": "cn" }
}
```

快速体验：

```bash
curl -s -XPOST localhost:8080/events -H 'Content-Type: application/json' -d '{
  "eventId":"e1","eventTimeMillis":1000,"traceId":"t1","spanId":"s1",
  "parentSpanId":null,"service":"svc-a","metric":"cpu","value":120,"tags":{"host":"h1"}}'
curl -s localhost:8080/metrics
```

---

## 2. 窗口与水位线规则（确定、可复现）

* **滚动窗口**：长度 `telemetry.window.size-millis`（默认 10s）。
  事件归属窗口起点：`windowStart = floor(eventTime / size) * size`，窗口为 `[start, end)`。
  归属只取决于事件时间，与到达顺序无关。
* **水位线**：观察到事件时间 `t` 后
  `watermark = max(watermark, maxEventTime - allowedLateness)`，单调不减。
* **窗口关闭**：当 `watermark >= windowEnd` 时窗口定稿（seal）。定稿后的聚合不可变，
  供查询与告警共同使用。
* **空闲推进**：`telemetry.window.advance-on-idle=true`（默认）时，即使无事件，
  水位线也会按墙上时钟以 `tick-interval` 周期推进到 `wallClock - allowedLateness`，
  保证无流量时窗口最终也关闭。置为 `false` 后水位线只由事件推进，
  测试因此可完全确定性复现。

### 迟到 / 重复 / 冲突（均不静默）

| 情形 | 结论 | RejectReason |
| --- | --- | --- |
| `eventTime < watermark - allowedLateness` | 超出迟到容忍边界 | `LATE_BEHIND_WATERMARK` |
| 所属窗口已按水位线定稿关闭（即便仍在容忍期内） | 不计入已定稿窗口 | `LATE_BEHIND_WATERMARK`（detail 注明“窗口已定稿关闭”） |
| 相同 eventId + 相同载荷（任意重试） | 返回首次结论 | 不产生新计数，计入 `duplicateReplayCount` |
| 相同 eventId + 不同载荷 | 标识冲突 | `CONFLICTING_EVENT_ID` |
| 字段缺失 / 时间为负 | 非法事件 | `INVALID_EVENT` |

幂等表按事件时间老化，但只回收**其窗口必然已定稿**的条目
（早于 `watermark - size - allowedLateness`），从机制上杜绝“老化后重试被二次计数”。

---

## 3. 并发模型、基数与背压策略

### 并发模型

* 调用方线程只做字段校验，随后进入**有界队列**（`buffer.queue-capacity`，默认 10000）。
* 单一 `telemetry-ingest-worker` 线程顺序完成：幂等判定 → 水位线推进 → 标签窗口计数
  → trace 窗口计数 → 调用链登记 → 窗口定稿 → 告警评估。
  单线程线性化从结构上消除计数丢失与状态错乱；累加器自身同步，保证并发查询读到一致快照。
* 同一事件对“标签聚合 / trace 聚合 / 调用链”三处的计入**要么全成功、要么全回滚**
  （失败时补偿撤销前两步），不会出现半计入。

### 背压（不无限缓冲、不长时间阻塞）

* 入队最多等待 `buffer.offer-timeout`（默认 50ms），超时以 `BUFFER_FULL` 快速失败；
  可配置为 0 表示纯非阻塞尝试。
* 调用方等待处理结果最多 `buffer.result-timeout`，超时返回 `PROCESSING_TIMEOUT`
  （瞬态、不登记；重试由幂等表给出确定结论）。
* `/metrics` 实时暴露 `queueDepth`、历史峰值 `queuePeak`。

### 基数与内存有界保护（全部可观测）

| 上限 | 配置项 | 触发时 RejectReason |
| --- | --- | --- |
| 去重事件 ID 池 | `cardinality.max-event-ids`（默认 20 万） | `TOO_MANY_EVENT_IDS`（不做静默 LRU 驱逐） |
| 单窗口标签序列数 | `cardinality.max-series-per-window`（默认 5000） | `TOO_MANY_SERIES` |
| 单窗口 trace 序列数 | `cardinality.max-trace-series-per-window`（默认=max-traces） | `TOO_MANY_SERIES` |
| 保留的定稿窗口数 | `cardinality.max-sealed-windows`（默认 1000） | 超出丢弃最老窗口并计数 `droppedSealedWindows` |
| 调用链数量 | `cardinality.max-traces`（默认 1 万） | `TOO_MANY_TRACES` |
| 单链 span 数 | `cardinality.max-spans-per-trace`（默认 200） | `TOO_MANY_SPANS` |
| 单事件标签数 / 长度 | `max-tags-per-event` / `max-tag-value-length` | `TAG_CARDINALITY_LIMIT` |
| 空闲调用链 | `trace.retention`（默认 10 分钟） | 到期回收 |

各拒绝原因的累计数量可在 `/metrics` 的 `rejectedByReason` 中观察。

---

## 4. 调用链还原

依据 `spanId / parentSpanId` 还原父子拓扑，任何异常都**明确标记而不丢数据、不中断判定**：

| 状态 | 含义 |
| --- | --- |
| `COMPLETE` | 唯一根 span，全部连通且无环 |
| `ORPHAN` | 存在 parent 缺失（乱序未到齐），或多根/无根；span 补齐后可恢复 COMPLETE |
| `CYCLE` | 父子边构成有向环（Kahn 拓扑检测），snapshot 的 reasons 列出成环 span |
| `DUPLICATE_SPAN` | 相同 spanId 重复出现（span 标识冲突） |

`GET /traces/{traceId}` 返回状态、span 列表与人类可读的判定依据（reasons）。

---

## 5. 告警稳定性规则

* 评估对象：规则 × 标签序列（`TAGS` 作用域）或规则 × traceId（`TRACE` 作用域）。
* 只由**定稿窗口**驱动，比较对象为窗口聚合的均值 `avg`（操作符 GT/GTE/LT/LTE）。
* 滞回状态机抑制阈值附近抖动：
  * `OK`：连续 `alert.breach-streak`（默认 2）个窗口越界才进入 `FIRING`；
    期间单点毛刺只到 `PENDING`，不对外报警，随后一个正常窗口即清零回 `OK`。
  * `FIRING`：连续 `alert.recover-streak`（默认 2）个窗口恢复才回 `OK`；
    单次回落维持 `FIRING`，不反复触发/恢复。
  * 状态翻转还受 `alert.cooldown`（默认 30s）冷却限制。
* 调用链作用域下，链路为 ORPHAN/CYCLE/DUPLICATE_SPAN 时**继续按现有数据评估**，
  并在告警快照 `traceMarker` 中标注拓扑异常，保证结论可解释。

### 规则配置

`telemetry.alert.rules`，分号分隔多条，逗号分隔字段：

```
id,scope,service,metric,threshold,operator,tagK=tagV,...
```

* scope：`TAGS` 或 `TRACE`；service/metric 留空表示匹配全部。
* 第 7 个字段起为可选标签选择器（仅 TAGS 生效）。
* 留空时使用内置默认规则 `default-avg-gt-100`（任意指标窗口均值 > 100）。

示例：

```properties
telemetry.alert.rules=cpu-hot,TAGS,svc-a,cpu,80,GT,host=h1;trace-slow,TRACE,svc-a,latency,200,GTE
```

---

## 6. 本地验证（测试）

```bash
mvn clean test
```

测试均关闭空闲水位线推进（`advance-on-idle=false`），用水位线只随事件时间前进的方式
保证确定性；每个用例的日志都会打印**输入事件**与**判定依据**（INGEST / ASSERT /
AGGREGATE / ALERT / STATE 行）。

| 测试类 | 覆盖场景 |
| --- | --- |
| `WindowWatermarkTest` | 窗口归属、水位线推进、乱序在容忍内、超出水位线 / 窗口已定稿的可区分拒绝 |
| `DedupRetryTest` | 任意次重试不双计数、同 ID 不同载荷冲突、确定性拒绝的幂等重放 |
| `ConcurrencyBackpressureTest` | 16 线程 × 500 并发计数精确无丢失；队列满快速 BUFFER_FULL；标签/trace/ID 池基数有界 |
| `TraceTopologyTest` | 孤立→乱序补齐→COMPLETE、有向环、重复 span、多根、trace 基数上限 |
| `AlertStabilityTest` | 单点毛刺不报警、连续确认、瞬时回落不恢复、冷却防抖、标签选择器作用域 |
| `TraceAlertTest` | ORPHAN/CYCLE 链路仍判定且带 traceMarker |
| `EndToEndIntegrationTest` | 经 HTTP 的摄取→聚合→告警→链路→指标全链路对齐；重复 / 迟到的 HTTP 语义 |

查看某次测试的输入与判定：

```bash
mvn test -Dtest=AlertStabilityTest
# 输出示例：
# INGEST eventId=... eventTime=... value=20.0 => ACCEPTED
# ALERT rule=high state=FIRING breachStreak=2 ...
# ASSERT w@80=5 => FIRING（瞬时回落不恢复）
```

# Telemetry Aggregator（本机运行的遥测聚合与告警）

一个基于 Spring Boot 3 / Java 21 风格（release 17 编译）的单机遥测聚合服务：
高并发摄取、按**事件时间**做窗口聚合、依据父子 span 还原调用链并进行稳定告警，
并支持**规则在线运营**（不重启调整阈值/匹配范围/启停/免打扰）与**按事件时间复盘审计**。
对外提供摄取 / 聚合 / 告警 / 链路 / 指标 / 规则运营 / 复盘七类 HTTP 接口。

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
| GET | `/alerts` | 查询全部规则 × 对象的告警状态与判定依据（含规则版本、是否免打扰） |
| GET | `/traces/{traceId}` | 查询调用链拓扑还原结果与状态原因 |
| GET | `/metrics` | 接受量、按原因区分的拒绝量、队列堆积、基数占用、水位线、复盘容量 |
| POST | `/rules` | 在线调整规则（阈值/范围/启停/免打扰），返回版本号与生效边界 |
| GET | `/rules/history` | 全部规则的版本历史（审计） |
| GET | `/rules/{id}/history` | 单条规则的版本历史与生效边界 |
| GET | `/replay?at=` | 按事件时间复盘；`traceId/service/metric` 可选过滤 |

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
* 单一 `telemetry-ingest-worker` 线程顺序完成：幂等判定 → 规则变更/事件计入 →
  水位线推进 → 标签窗口计数 → trace 窗口计数 → 调用链登记 → 窗口定稿 → 告警评估
  → 复盘日志追加。**在线规则变更与事件写入共用同一队列、同一 worker 与同一全局序列号**，
  单线程线性化从结构上消除计数丢失、状态错乱与新旧规则互相覆盖；累加器自身同步，
  保证并发查询读到一致快照。
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

### 规则配置（启动初始规则）

`telemetry.alert.rules` 只定义**启动时的 v1 初始规则**，分号分隔多条，逗号分隔字段：

```
id,scope,service,metric,threshold,operator,tagK=tagV,...
```

* scope：`TAGS` 或 `TRACE`；service/metric 留空表示匹配全部。
* 第 7 个字段起为可选标签选择器（仅 TAGS 生效）。
* 留空时使用内置默认规则 `default-avg-gt-100`（任意指标窗口均值 > 100）。
* 启动后的一切阈值/范围/启停/免打扰调整都通过 `POST /rules` 在线完成，
  不需要修改本配置、不需要重启；初始规则与在线版本共用同一条版本历史。

示例：

```properties
telemetry.alert.rules=cpu-hot,TAGS,svc-a,cpu,80,GT,host=h1;trace-slow,TRACE,svc-a,latency,200,GTE
```

### 5.1 规则版本与生效边界（在线运营，不重启不断流）

规则不再是静态配置：每次 `POST /rules` 变更都产生一个**不可变新版本**，版本号单调递增，
与事件写入进入**同一个有界队列、由同一个 worker 线性化**，因此变更与事件并发发生时
不会出现状态丢失、重复计数或新旧规则互相覆盖。

* **生效边界（事件时间语义）**：worker 处理变更时取当前水位线作为 `effectiveFrom`。
  窗口在 `windowEnd` 定稿时，解析 **`effectiveFrom < windowEnd` 的最新版本**作为判定依据。
  因此**边界之前已经定稿的窗口维持原判（不追溯改写），边界之后定稿的窗口按新版本判定**。
* 可调整内容：`threshold`、`operator`、`scope`、`service`、`metric`、`tagSelector`
  （匹配范围）、`enabled`（启停）、`quietFromMillis/quietUntilMillis`（免打扰区间）。
  请求中未提供的字段沿用上一版本；规则不存在则新建。
* **启停**：停用后后续定稿窗口不再评估该规则，主体状态与证据原样保留；重新启用后继续。
* **幂等**：请求可带 `requestId`，相同 `requestId` 的任意重试返回首次结论（含相同版本号
  与相同生效边界），不会产生第二个版本。
* 非法请求（缺规则 ID、阈值非有限数、免打扰区间倒置等）返回 `422 INVALID_REQUEST`，
  不产生版本；队列满返回 503、等待超时返回 504，均为可重试的可区分结论。

请求体示例：

```json
{
  "requestId": "chg-20261003-01",
  "id": "cpu-hot",
  "threshold": 70,
  "operator": "GTE",
  "tagSelector": { "host": "h1" },
  "enabled": true,
  "quietFromMillis": 1727000600000,
  "quietUntilMillis": 1727000900000,
  "note": "值班窗口降阈值并设置00:10-00:15免打扰"
}
```

响应含 `version` 与 `effectiveFromMillis`；`GET /rules/{id}/history` 可列出该规则全部版本
（阈值、范围、启停、免打扰区间、生效边界、变更说明），供复盘审计引用。

### 5.2 免打扰语义

免打扰是绑定在**规则版本**上的一段事件时间半开区间 `[quietFrom, quietUntil)`，
随在线变更即时生效，无需重启：

* 与区间相交的定稿窗口，**证据照常累计**（越界计数 breachStreak、恢复计数
  recoverStreak 都继续走），但**不发生对外状态翻转**：
  不会新进入 FIRING，也不会从 FIRING 回到 OK；`/alerts` 中 `quietActive=true`、
  `suppressedCount` 记录被抑制的窗口数。
* **区间结束后按当前已累计证据平稳恢复**：下一个定稿窗口沿用区间内的 streak 继续状态机，
  每个窗口最多翻转一次，因此**不会在免打扰结束瞬间补发一串告警**。
* 取消免打扰：再提交一次变更，把 `quietFromMillis` 与 `quietUntilMillis` 显式置为 `0`。

---

## 6. 按事件时间复盘审计

`GET /replay?at=<事件时间毫秒>` 返回某个事件时间点 T 的完整结论：
那一刻**已定稿**的窗口聚合、当时生效的**规则版本**、对应**告警状态**（含每步判定依据）
与**调用链结论**（ORPHAN/CYCLE/DUPLICATE_SPAN 的原因同样可见）。可选参数
`traceId`、`service`、`metric` 用于缩小输出。

### 结论如何保证确定、可复现、与实时对得上

* 复盘**只由输入事件与规则变更历史决定**：摄取 worker 给事件与规则变更分配同一个
  严格递增的全局 `seq`，并记录“处理完该事件后的水位线”。复盘按 `seq` 顺序重放，
  没有墙上时钟、没有随机、没有空闲回收，且复用与实时完全相同的计入/定稿/状态机代码。
  **同一批输入无论何时执行、在哪台机器执行，结论逐字节一致**。
* **切点语义**：找到“处理后水位线首次 ≥ T”的事件 seq（cutoff），重放 ≤cutoff 的事件，
  并按该事件处理后的水位线关闭窗口——这正是实时系统在该线性化点对外可见的状态，
  因此同一时间边界下复盘结论与当时实时已经产生的结果严格一致；T 尚未被任何输入越过时
  返回 `PARTIAL`（只含当时确实已定稿的窗口，不凭空关闭未来窗口）。
* **只读、隔离**：复盘使用全新的本地水位线/窗口存储/链路表/规则版本/状态机对象，
  绝不触碰实时状态（不推进实时水位线、不改计数、不写日志）。
* **跨视图一致**：同一事件/窗口/调用链在聚合、链路、告警三视图以及复盘视图中的计入与
  排除结论一致；被拒绝事件在任何视图都不计入，ORPHAN/CYCLE/DUPLICATE_SPAN 在每个视图
  都带相同异常标记，不会“一个视图当正常、另一个说不存在”。

### 可复盘范围与容量限制（不返回缺数据的“完整”答案）

复盘日志是**有界环形缓冲**（`telemetry.replay.*`）：

| 边界 | 配置项 | 超出时行为 |
| --- | --- | --- |
| 保留的已接受事件数 | `replay.max-journal-events`（默认 20 万） | 丢弃最老事件并推进可复盘下界 |
| 告警判定事实条数 | `replay.max-alert-facts`（默认 20 万） | 丢弃最老事实（在线状态不受影响） |
| 单次扫描事件数 | `replay.max-scan-events-per-replay`（默认 20 万） | 返回 503 `RESOURCE_LIMITED` |
| 并发复盘数 | `replay.max-concurrent-replays`（默认 1，串行） | 返回 503 `RESOURCE_LIMITED` |
| 单次返回调用链数 | `replay.max-traces-per-report`（默认 200） | 截断（报告其余部分完整） |

* 事件老化时会在推进后的水位线处固化一次**告警状态检查点**，使保留下界之后的复盘仍能
  正确延续 streak/状态；但**早于可复盘下界的时间点无法复盘**，接口返回
  `404 NOT_REPLAYABLE` 并在 `unavailableReason` 中明确说明，绝不给出看似完整、实际缺
  数据的答案。当前下界可在 `/metrics` 的 `replayHorizonMillis` 观察。
* 复盘在独立的只读重放路径上串行执行、不与摄取线程争抢共享状态；即使复盘被限流或超时，
  实时摄取仍照常工作。`/metrics` 另暴露 `journalEventCount`、`journalDroppedEvents`、
  `alertFactCount`、`ruleVersionCount`、`activeReplays` 等容量指标。

---

## 7. 本地验证（测试）

```bash
mvn clean test
```

测试均关闭空闲水位线推进（`advance-on-idle=false`），用水位线只随事件时间前进的方式
保证确定性；每个用例的日志都会打印**输入事件、规则版本和每一步的判定依据**
（INGEST / CHANGE / ASSERT / AGGREGATE / ALERT / RULE-VERSION / REPLAY-ALERT 行）。

| 测试类 | 覆盖场景 |
| --- | --- |
| `WindowWatermarkTest` | 窗口归属、水位线推进、乱序在容忍内、超出水位线 / 窗口已定稿的可区分拒绝 |
| `DedupRetryTest` | 任意次重试不双计数、同 ID 不同载荷冲突、确定性拒绝的幂等重放 |
| `ConcurrencyBackpressureTest` | 16 线程 × 500 并发计数精确无丢失；队列满快速 BUFFER_FULL；标签/trace/ID 池基数有界 |
| `TraceTopologyTest` | 孤立→乱序补齐→COMPLETE、有向环、重复 span、多根、trace 基数上限 |
| `AlertStabilityTest` | 单点毛刺不报警、连续确认、瞬时回落不恢复、冷却防抖、标签选择器作用域 |
| `TraceAlertTest` | ORPHAN/CYCLE 链路仍判定且带 traceMarker |
| `EndToEndIntegrationTest` | 经 HTTP 的摄取→聚合→告警→链路→指标全链路对齐；重复 / 迟到的 HTTP 语义 |
| `RuleOnlineChangeTest` | 阈值/范围/启停/免打扰在线调整、版本与生效边界、边界两侧新旧规则、requestId 幂等、非法请求 |
| `RuleChangeConcurrencyTest` | 8 摄取线程与规则变更线程并发：计数精确、版本连续、seq 唯一线性化、边界不被穿越 |
| `ReplayDeterminismTest` | 多次/多实例复盘逐字节一致、与实时对得上、边界前维持 v1、只读不改实时态、NOT_REPLAYABLE、RESOURCE_LIMITED |
| `CrossViewConsistencyTest` | 事件/窗口/链路在聚合·链路·告警·复盘视图的计入一致；ORPHAN/CYCLE/DUPLICATE_SPAN/拒绝在各视图一致 |
| `QuietRecoveryTest` | 免打扰内只累计证据不翻转、结束后单次平稳 FIRING/恢复不补发、FIRE 中免打扰不抖动、取消免打扰 |
| `FeatureRegressionTest` | 版本化链路上迟到/重复/标识冲突/阈值抖动全部回归，且复盘与实时一致 |
| `RuleReplayHttpIntegrationTest` | 经 HTTP 在线改规则、版本历史、`/replay` 完整/404/422 语义 |

查看某次测试的输入、规则版本与判定：

```bash
mvn test -Dtest=RuleOnlineChangeTest,ReplayDeterminismTest
# 输出示例：
# CHANGE APPLIED v=2 effectiveFrom=11 | 阈值 100.0->10.0
# INGEST eventId=a20 eventTime=21 value=50.0 => ACCEPTED
# ALERT rule=r1@v2 state=FIRING basis=[window=[20,30) ... => OK->FIRING]
# RULE-VERSION id=r1 v=2 threshold=10.0 effectiveFrom=11
# REPLAY-ALERT rule=r1@v1 state=OK quiet=false basis=[window=[0,10) ...]
```

### 本地手工验证在线运营与复盘

```bash
mvn spring-boot:run
# 1) 发事件并关闭窗口0（旧阈值 100，50 不报警）
curl -s -XPOST localhost:8080/events -H 'Content-Type: application/json' -d \
 '{"eventId":"a0","eventTimeMillis":1,"traceId":"t0","spanId":"s","service":"svc","metric":"m","value":50,"tags":{}}'
curl -s -XPOST localhost:8080/events -H 'Content-Type: application/json' -d \
 '{"eventId":"f0","eventTimeMillis":11,"traceId":"tf","spanId":"s","service":"svc","metric":"z","value":0,"tags":{}}'
# 2) 不重启把阈值降到 10
curl -s -XPOST localhost:8080/rules -H 'Content-Type: application/json' -d \
 '{"requestId":"demo-1","id":"default-avg-gt-100","threshold":10,"note":"值班降阈值"}'
# 3) 查版本历史与生效边界
curl -s localhost:8080/rules/default-avg-gt-100/history
# 4) 再发窗口20事件（50 > 10）后复盘 T=11 与 T=31 的差异
curl -s "localhost:8080/replay?at=11"   # 窗口0 按 v1(阈值100) 判定 OK
curl -s "localhost:8080/replay?at=31"   # 窗口20 按 v2(阈值10) 判定 FIRING，且依据可逐条查看
```

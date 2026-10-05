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
| POST | `/rules/changes` | 在线变更告警规则（阈值/范围/启停/免打扰），无需重启；按 requestId / Idempotency-Key 幂等，返回版本号、生效边界与 replayed；失败分 422 非法 / 503 背压 / 408 超时三类 |
| GET | `/rules` | 当前生效规则集（含版本与免打扰边界） |
| GET | `/rules/history` | 全部规则变更历史（版本升序，含每次的生效边界） |
| GET | `/replay?asOfMillis=` | 按事件时间边界复盘：已定稿窗口聚合、当时规则版本、告警状态、链路结论 |

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
    冷却与全部状态机计时均以**事件时间**（定稿窗口 end）为基准，与墙上时钟无关，
    这是复盘可逐位复现告警结论的前提。
* 调用链作用域下，链路为 ORPHAN/CYCLE/DUPLICATE_SPAN 时**继续按现有数据评估**，
  并在告警快照 `traceMarker` 中标注拓扑异常，保证结论可解释。

### 告警结论字段与判定依据（evidence）

`GET /alerts` 与复盘报告中的每条告警（`AlertSnapshot`）除了状态与计数，还携带
**按事件时间升序的判定依据链** `evidence`（最近 `telemetry.alert.evidence-trail-size`
条，默认 64，实时与复盘共用同一上界）。每条 `evidence` 完整回答“这一步是怎么判的”：

| 字段 | 含义 |
| --- | --- |
| `windowStartMillis` / `windowEndMillis` | 依据的窗口 `[start,end)`（事件时间）；end 同时是状态机的“现在”与生效边界判定基准 |
| `avg` / `count` | 该窗口参与比较的聚合均值与样本数（即“哪份证据”） |
| `ruleVersion` / `threshold` / `operator` / `service` / `metric` | 该窗口实际使用的规则版本、阈值、操作符与匹配范围 |
| `breach` | 均值在该阈值/操作符下是否越界 |
| `consecutiveBreaches` / `consecutiveRecoveries` | 评估后的连续越界 / 连续恢复计数（毛刺确认、平稳恢复均可读） |
| `stateBefore` / `stateAfter` / `transition` | 评估前后状态（OK/PENDING/FIRING）及本次是否翻转——可解释“为什么从正常翻成告警” |
| `suppressed` | 已满足确认次数但被冷却（cooldown）抑制、维持原状态 |
| `muted` / `muteUntilMillis` | 本次是否处在免打扰区间及其截止；免打扰窗口 streak 归 0、`transition=false`，文案明确“越界但被抑制、不累计” |
| `traceMarker` | TRACE 作用域下的拓扑异常标记（ORPHAN/CYCLE/DUPLICATE_SPAN） |
| `rationale` | 人类可读的一句话判定说明（窗口、均值、阈值版本、计数、翻转/抑制/免打扰原因） |

依据链只由事件时间、窗口聚合与规则版本历史决定，不含墙上时钟/线程/随机量：
同一批输入在同一时间边界下，**实时产生的结论与复盘结论（含整条 evidence）逐位一致**；
重复复盘、在不同机器/不同时刻复盘也完全相同。

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

## 6. 规则在线变更：版本、生效边界、幂等与失败语义

无需重启、无需断流即可调整告警规则。变更与事件写入**共用同一条有界队列**，
在单一 worker 线程内与事件处理线性化：应用变更前先把所有可定稿窗口按旧规则定稿，
再以当前水位线作为**生效边界**登记新版本，因此不会出现状态丢失、重复计数或新旧规则互相覆盖。

* **生效边界语义**：变更记录 `effectiveFromMillis` = 应用时刻的事件时间水位线。
  窗口结束时间 `end <= effectiveFrom` 的窗口在此之前已定稿，**维持原判**；
  `end > effectiveFrom` 的窗口按新规则判定。实时评估与复盘共用同一解析函数
  （`RuleRegistry.rulesForWindow(windowEnd)`），保证两侧结论一致。
* **版本**：每次**不同的成功变更**得到单调递增的 `version`（初始规则集为 0），
  `GET /rules/history` 可查询全部版本及其生效边界；告警快照携带 `ruleVersion`，
  可说明每条结论依据的是哪个版本。
* **动作**：`UPSERT`（新建/更新阈值、匹配范围、操作符）、`DISABLE` / `ENABLE`
  （停用期间不评估新窗口，主体状态保留，启用后续接）、`MUTE` / `UNMUTE`（见第 7 节）。

### 6.1 幂等重试（同一变更任意次重发只落一个版本）

每次变更请求由调用方携带**幂等键** `requestId`（推荐为这次“逻辑变更”生成一个 UUID，
重试时保持不变；也可用 HTTP 头 `Idempotency-Key`）。服务端在 worker 线性化点按键去重：

* 同一 `requestId` + 相同内容的请求（顺序重试、超时重发、并发重发）**永远只产生一个版本**，
  每次返回**同一个版本号、同一个生效边界、同一份规则快照**；
  响应中的 `replayed=false` 表示首次生效，`replayed=true` 表示此前已生效、本次为无副作用重放。
* 重试命中时**不会再次定稿窗口、不会第二次改动规则状态**：
  边界之前已定稿窗口的结论、边界之后窗口的判定都不随重试变化。
* 同一 `requestId` 但内容不同（想复用键提交另一条变更）按非法变更拒绝（422），不产生版本。
* 幂等登记表有界（`telemetry.rules.max-idempotency-keys`，默认 2000，超出淘汰最老键），
  其生命周期独立于版本历史折叠；请在保留窗口内完成重试。
* 不显式携带键时，服务端按请求体内容生成一个内容键——只保证**字节级完全相同的重发**幂等；
  生产调用方应始终显式携带 `requestId`。
* 变更与并发摄取同时发生时，不同变更仍各自占一个版本，版本号连续、不丢失、不互相覆盖
  （与事件计数同一条线性化队列保证）。

成功响应（顶层即版本字段，向后兼容）：

```json
{
  "version": 1,
  "requestId": "8f1c...",
  "replayed": false,
  "ruleId": "r",
  "action": "UPSERT",
  "ruleSnapshot": { "id": "r", "scope": "TAGS", "service": "svc", "metric": "m",
                    "tagSelector": {}, "threshold": 100.0, "operator": "GT" },
  "muteUntilMillis": null,
  "effectiveFromMillis": 11,
  "appliedAtMillis": 1780000000000
}
```

### 6.2 失败语义（三类可区分结果）

| 情形 | HTTP | error | applied | 含义与处理 |
| --- | --- | --- | --- | --- |
| 非法变更（未知规则、缺定义、id 不一致、同键不同载荷、MUTE 边界非法） | 422 | `RULE_CHANGE_INVALID` | false | **确定无版本、无任何状态改动**，快速拒绝；不要用同键重试，应修正请求后换新键 |
| 背压（队列满，未能入队） | 503 | `RULE_CHANGE_BACKPRESSURE` | false | **确定未生效**（未登记、无版本）；稍后用**同一 requestId** 重试安全 |
| 等待回执超时 | 408 | `RULE_CHANGE_TIMEOUT` | `"UNKNOWN"` | **结论不确定**：可能根本未生效，也可能已生效只是回执丢失。**必须用同一 requestId 重试**——未生效则首次应用并返回版本；已生效则 `replayed=true` 重放同一版本，绝不会产生第二个版本 |

超时返回时服务端**不会取消**正在排队/执行的变更任务，因此“先超时后生效”是正常情形，
依靠同键重试即可收敛到唯一确定的结论；调用方不需要、也不应该猜测是否生效。

```bash
# 推荐：显式携带幂等键；超时/背压后用同一个 requestId 原样重发
curl -s -XPOST localhost:8080/rules/changes \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: 8f1c2e7a' -d '{
  "ruleId":"r","action":"UPSERT",
  "rule":{"id":"r","scope":"TAGS","service":"svc","metric":"m",
          "tagSelector":{},"threshold":100.0,"operator":"GT"}}'
# 免打扰到事件时间 1727000100000
curl -s -XPOST localhost:8080/rules/changes -H 'Content-Type: application/json' -d '{
  "requestId":"mute-2026-04-01","ruleId":"r","action":"MUTE","muteUntilMillis":1727000100000}'
```

---

## 7. 免打扰语义（MUTE）

* 免打扰区间按**事件时间**判定：窗口 `end <= muteUntilMillis` 的定稿窗口被抑制——
  只记录证据（lastValue/lastWindow 照常更新），**不推进状态机**，快照标记 `muted=true`。
* 抑制期间越界/恢复次数**不累计**（streak 清零），因此免打扰结束后必须按当前证据
  重新累计 `breach-streak` / `recover-streak` 个连续窗口才翻转状态——
  **平稳恢复，绝不一次性补发**期间累积的告警。
* 已进入 FIRING 的对象在免打扰期间保持 FIRING 且 `muted=true`（不抹掉既有状态，
  也不产生新的翻转）；`UNMUTE` 或区间结束后下一窗口立即恢复正常判定。

---

## 8. 按事件时间复盘（Replay）

`GET /replay?asOfMillis=T` 对事件时间边界 T 重放**与实时完全相同的处理管道**
（`EventProcessor`），在独立的状态副本上重建：

* 边界内已定稿的窗口聚合（`end <= 复盘水位线`）；
* 每个窗口当时生效的规则版本（按生效边界解析）与对应告警状态（含 `ruleVersion`、`muted`
  及与实时逐位一致的完整判定依据链 `evidence`）；
* 调用链拓扑结论（COMPLETE/ORPHAN/CYCLE/DUPLICATE_SPAN 及判定依据）；
* 边界内被确定拒绝的事件按原因计数（审计依据）。

**确定性与兼容范围**：

* 复盘结果只由**事件日志**（每条已提交结论：接受或确定性拒绝）与**规则变更历史**决定；
  告警状态机只使用事件时间（定稿窗口 end），与墙上时钟无关，因此同一批输入
  无论何时、在哪台机器上复盘都得到同一结论；复盘不读写任何实时状态。
* 复盘只接受事件时间 `<= T` 的证据，水位线由这批事件独立推进；
  与实时在同一事件时间边界下产生的结论一致（实时/复盘一致性由
  `RuleChangeConcurrencyTest` 的逐位比对保证）。
* **不可复盘即明确拒绝**：`status=OUT_OF_RETENTION` 并说明原因，绝不给出看似完整、
  实际缺数据的答案。触发条件：
  * 事件日志容量（`telemetry.journal.max-events`，默认 10 万条）淘汰的最老事件时间
    早于等于 T；
  * 规则版本历史（`telemetry.rules.max-versions`，默认 1000）折叠后，
    边界内窗口所需的旧版本已不可考。
* **资源边界**：单次复盘最多处理 `telemetry.replay.max-events`（默认 20 万）条事件，
  超出返回 `TOO_LARGE`；同时最多一个复盘在执行（其余快速失败 `BUSY`）；
  调用方等待受 `telemetry.replay.timeout`（默认 30s）限制。
  复盘在独立线程与独立状态副本上运行，不拖慢实时摄取。

```bash
curl -s 'localhost:8080/replay?asOfMillis=1727000000000'
```

---

## 9. 本地验证（测试）

```bash
mvn clean test
```

测试均关闭空闲水位线推进（`advance-on-idle=false`），用水位线只随事件时间前进的方式
保证确定性；每个用例的日志都会打印**输入事件**、**用到的规则版本**与**每一步的判定依据**
（INGEST / ASSERT / AGGREGATE / ALERT / STATE 行，以及 `BASIS` / `REPLAY_BASIS`
逐条判定依据：窗口、均值、阈值版本、streak、翻转/冷却抑制/免打扰原因；
规则变更打印 `RULE_CHANGE requestId=… replayed=… version=… effectiveFrom=…`）。

| 测试类 | 覆盖场景 |
| --- | --- |
| `WindowWatermarkTest` | 窗口归属、水位线推进、乱序在容忍内、超出水位线 / 窗口已定稿的可区分拒绝 |
| `DedupRetryTest` | 任意次重试不双计数、同 ID 不同载荷冲突、确定性拒绝的幂等重放 |
| `ConcurrencyBackpressureTest` | 16 线程 × 500 并发计数精确无丢失；队列满快速 BUFFER_FULL；标签/trace/ID 池基数有界 |
| `TraceTopologyTest` | 孤立→乱序补齐→COMPLETE、有向环、重复 span、多根、trace 基数上限 |
| `AlertStabilityTest` | 单点毛刺不报警、连续确认、瞬时回落不恢复、冷却防抖、标签选择器作用域 |
| `TraceAlertTest` | ORPHAN/CYCLE 链路仍判定且带 traceMarker |
| `EndToEndIntegrationTest` | 经 HTTP 的摄取→聚合→告警→链路→指标全链路对齐；重复 / 迟到的 HTTP 语义 |
| `RuleHotUpdateTest` | 阈值在线调整的生效边界（边界前维持原判、边界后新规则）、启停续接、非法变更拒绝 |
| `MuteRecoveryTest` | 免打扰抑制判定、结束后平稳恢复不补发、UNMUTE 立即恢复 |
| `RuleChangeIdempotencyTest` | 同 requestId 顺序/并发重试只落一个版本且返回一致；生效后重试不重新定稿、不改判定；变更与并发摄取交错版本连续；背压（确定未生效）与超时（结论不确定）的可区分语义及同键重试收敛；同键不同载荷/非法变更拒绝且零副作用 |
| `AlertEvidenceTest` | 判定依据完整性：OK→PENDING→FIRING 每步窗口/聚合/版本/阈值/计数可读；免打扰窗口标记 muted 且不累计；边界前后版本切换；实时=复盘、重复复盘依据逐位一致；每条告警带非空 rationale |
| `RealtimeReplayAlignmentTest` | 多规则多对象 + 变更 + 免打扰下，同一时间边界实时与复盘的每条告警（状态/版本/计数/完整依据）逐条对得上；定稿聚合一并对齐 |
| `RuleChangeConcurrencyTest` | 8 线程并发摄取 + 20 次并发规则变更：计数守恒、版本连续、实时/复盘三视图逐位一致 |
| `ReplayDeterminismTest` | 重复复盘逐位一致、与当时实时结论对齐、不改写实时状态、日志/规则历史超范围明确拒绝 |
| `CrossViewConsistencyTest` | 同一事件/窗口/链路在聚合、链路、告警、复盘视图中的计入/排除与异常标记一致 |
| `RuleReplayHttpTest` | HTTP 层规则变更、版本历史、复盘与 422 错误语义；requestId / Idempotency-Key 幂等（replayed 标志、同键冲突 422）；告警/复盘的 evidence 字段 |

查看某次测试的输入、规则版本与每步判定依据：

```bash
mvn test -Dtest=AlertEvidenceTest
# 输出示例：
# RULE_CHANGE requestId=up-1 replayed=false version=1 rule=r action=UPSERT effectiveFrom=11 ...
# BASIS rule=r subject={} v1 window=[20,30) avg=20.0/n=1 GT 100.0 => FIRING->OK muted=false ...
#   | 窗口[20,30) avg=20.0 未越界（GT 阈值 100.0…）；连续恢复 1/1 且冷却已满 => FIRING 恢复为 OK
# REPLAY_BASIS rule=r ...（与实时同一步逐位相同）
```

#!/usr/bin/env bash
# 本机一键验证：启动服务 -> 造数 -> 校验摄取/去重/乱序链路/告警/水位线/迟到/统计 -> 关闭
set -u
BASE="http://localhost:${PORT:-18090}/telemetry"
JAR="target/telemetry-aggregator-java-1.0-SNAPSHOT.jar"
APP_LOG="/tmp/telemetry-verify.log"
PID=""
FAIL=0

say()  { printf '\n=== %s ===\n' "$1"; }
check(){ # check <描述> <实际> <期望(子串)>
  if [[ "$2" == *"$3"* ]]; then echo "PASS: $1"; else echo "FAIL: $1 (got: $2)"; FAIL=1; fi
}
# 摄取 POST 返回仅代表已入队；查询走优先控制队列，排空其执行前已入队的事件。
# 用带重试的查询等待期望结果出现（最多 ~3s）。
wait_for(){ # wait_for <url> <期望子串>
  local url="$1" want="$2" body=""
  for _ in $(seq 1 30); do
    body=$(curl -s "$url")
    [[ "$body" == *"$want"* ]] && { echo "$body"; return 0; }
    sleep 0.1
  done
  echo "$body"; return 0
}

say "构建"
mvn -q package -DskipTests || { echo "构建失败"; exit 1; }

say "启动服务"
java -jar "$JAR" --server.port="${PORT:-18090}" > "$APP_LOG" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null' EXIT
for _ in $(seq 1 40); do
  curl -s -o /dev/null "$BASE/stats" && break
  sleep 0.5
done

post(){ curl -s -X POST "$BASE/events" -H 'Content-Type: application/json' -d "$1"; }

say "1) 正常摄取 + 重复不双计 + 标识冲突 + 非法"
R1=$(post '{"eventId":"v1","timestampMillis":1200,"metric":"cpu","value":10,"tags":{"host":"a"}}')
check "首次接受" "$R1" '"status":"ACCEPTED"'
RDUP=$(post '{"eventId":"v1","timestampMillis":1200,"metric":"cpu","value":10,"tags":{"host":"a"}}')
check "重复回放接受结论(不双计)" "$RDUP" '"status":"ACCEPTED"'
RCONF=$(post '{"eventId":"v1","timestampMillis":1200,"metric":"cpu","value":11,"tags":{"host":"a"}}')
check "同id不同负载=ID_CONFLICT" "$RCONF" '"status":"ID_CONFLICT"'
RINV=$(post '{"eventId":"","timestampMillis":1200,"metric":"cpu"}')
check "空eventId=INVALID" "$RINV" '"status":"INVALID"'

say "2) 乱序调用链（子先到，根后到）+ 3) 告警防抖"
# 规则必须先注册，才会对随后到达的每个 span 做评估
# （连续2次>=200触发；连续2次<=150恢复；滞后带 (150,200)）
curl -s -X POST "$BASE/alerts/rules" -H 'Content-Type: application/json' \
  -d '{"ruleId":"slow","traceId":"t1","metric":"CHAIN_DURATION","fireThreshold":200,"resolveThreshold":150,"fireConsecutive":2,"resolveConsecutive":2}' >/dev/null

post '{"eventId":"p1","timestampMillis":1400,"metric":"latency","value":80,"tags":{"svc":"a"},"traceId":"t1","spanId":"c1","parentSpanId":"root","durationMillis":80}' >/dev/null
post '{"eventId":"p2","timestampMillis":1410,"metric":"latency","value":130,"tags":{"svc":"a"},"traceId":"t1","spanId":"c2","parentSpanId":"root","durationMillis":130}' >/dev/null
WAITING=$(wait_for "$BASE/chains/t1" '"status":"AWAITING_PARENT"')
check "父缺失=AWAITING_PARENT" "$WAITING" '"status":"AWAITING_PARENT"'
post '{"eventId":"p3","timestampMillis":1390,"metric":"latency","value":80,"tags":{"svc":"a"},"traceId":"t1","spanId":"root","durationMillis":80}' >/dev/null
COMPLETE=$(wait_for "$BASE/chains/t1" '"status":"COMPLETE"')
check "根到达=COMPLETE" "$COMPLETE" '"status":"COMPLETE"'

# 评估序列（每来一个 span 用当前链路总耗时评估一次）：
# p1=130(<=恢复阈值150,OK) p2=260(第1次高) p3(根)=340(第2次高)->FIRING
# p4=430(持续高位) p5=360(落入滞后带(150,200)?否,仍高) -> 均不得重复触发
FIRING=$(wait_for "$BASE/alerts/events?limit=5" '"state":"FIRING"')
check "连续2次超阈值=FIRING" "$FIRING" '"state":"FIRING"'
post '{"eventId":"p4","timestampMillis":1420,"metric":"latency","value":90,"tags":{"svc":"a"},"traceId":"t1","spanId":"c3","parentSpanId":"root","durationMillis":90}' >/dev/null
# p5 只加 1ms：总耗时 431，仍高位 —— 持续高位不得重复产生 FIRING
post '{"eventId":"p5","timestampMillis":1430,"metric":"latency","value":1,"tags":{"svc":"a"},"traceId":"t1","spanId":"c4","parentSpanId":"root","durationMillis":1}' >/dev/null
wait_for "$BASE/chains/t1" '"spanId":"c4"' >/dev/null
COUNT=$(curl -s "$BASE/alerts/events?limit=5" | grep -o '"state":"FIRING"' | wc -l)
check "持续高位/滞后带内抖动不重复触发(仍1条)" "$COUNT" "1"

say "4) 推进水位线关窗 + 迟到拒绝"
curl -s -X POST "$BASE/watermark/advance?eventTimeMillis=3000" >/dev/null
WINDOWS=$(curl -s "$BASE/windows")
check "窗口[1000,2000)已关闭且cpu计数=2" "$WINDOWS" '"metric":"cpu"'
LATE=$(post '{"eventId":"old1","timestampMillis":500,"metric":"cpu","value":1,"tags":{"host":"z"}}')
check "旧事件=LATE_BEHIND_WATERMARK" "$LATE" '"status":"LATE_BEHIND_WATERMARK"'

say "5) stats 可观察：拒绝原因计数 + 堆积量"
STATS=$(curl -s "$BASE/stats")
check "ID_CONFLICT 计数=1" "$STATS" '"ID_CONFLICT":1'
check "LATE 计数=1" "$STATS" '"LATE_BEHIND_WATERMARK":1'
check "INVALID 计数=1" "$STATS" '"INVALID":1'
check "幂等回放计数=1" "$STATS" '"dedupReplays":1'
check "队列已排空" "$STATS" '"queueDepth":0'

say "关闭服务"
kill $PID 2>/dev/null; PID=""
trap - EXIT

if [[ $FAIL -eq 0 ]]; then echo; echo "ALL CHECKS PASSED"; else echo; echo "SOME CHECKS FAILED (see app log: $APP_LOG)"; exit 1; fi

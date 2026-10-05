package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceSnapshot;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 规则变更与并发摄取同时发生：变更与事件写入在同一线性化点应用，
 * 不得出现状态丢失、重复计数或新旧规则互相覆盖；
 * 最终实时结论必须与同边界复盘结论逐位一致。
 */
class RuleChangeConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(RuleChangeConcurrencyTest.class);

    private static final int THREADS = 8;
    private static final int PER_THREAD = 250;
    private static final int TOTAL = THREADS * PER_THREAD;
    private static final int CHANGES = 20;

    @Test
    void concurrentIngestionAndRuleChangesStayConsistent() throws Exception {
        TelemetryProperties props = TestHarness.defaultProps();
        props.getWindow().setSizeMillis(10);
        props.getWindow().setAllowedLatenessMillis(50);
        props.getAlert().setBreachStreak(1);
        props.getAlert().setRecoverStreak(1);
        AlertRule initial = new AlertRule("r", AlertRule.Scope.TAGS, null, "m",
                Map.of(), 100.0, AlertRule.Operator.GT);

        try (TestHarness.Engine h = new TestHarness.Engine(props, List.of(initial), 0L)) {
            log.info("[CASE] {} 线程 × {} 事件并发摄取，同时在线提交 {} 次阈值变更",
                    THREADS, PER_THREAD, CHANGES);
            AtomicLong clock = new AtomicLong(0);
            CountDownLatch start = new CountDownLatch(1);
            List<Thread> threads = new ArrayList<>();

            for (int t = 0; t < THREADS; t++) {
                final int tid = t;
                Thread th = new Thread(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < PER_THREAD; i++) {
                            long ts = clock.incrementAndGet();
                            h.engine.ingest(TestHarness.event(
                                    "e-" + tid + "-" + i, ts, "tr-" + tid + "-" + (i % 50),
                                    "s" + (i % 3), null,
                                    "svc-" + (tid % 2), "m", ts % 200, Map.of()));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, "ingest-" + t);
                threads.add(th);
                th.start();
            }
            Thread changer = new Thread(() -> {
                try {
                    start.await();
                    for (int c = 0; c < CHANGES; c++) {
                        double threshold = c % 2 == 0 ? 50.0 : 150.0;
                        h.engine.submitRuleChange(new RuleChangeRequest(
                                "chg-" + c, "r",
                                RuleChange.Action.UPSERT,
                                new AlertRule("r", AlertRule.Scope.TAGS, null, "m",
                                        Map.of(), threshold, AlertRule.Operator.GT),
                                null));
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "rule-changer");
            changer.start();

            start.countDown();
            for (Thread th : threads) {
                th.join(TimeUnit.SECONDS.toMillis(30));
            }
            changer.join(TimeUnit.SECONDS.toMillis(30));

            // 推进水位线，定稿全部窗口
            long maxT = clock.get();
            h.send(log, TestHarness.event("flush", maxT + 10_000, "tr-flush", "s", null,
                    "svc-flush", "other", 0, Map.of()));

            // 1) 计数守恒：接受 + 确定拒绝 = 全部输入；日志与幂等表一一对应，无丢失无重复
            long accepted = h.metrics.acceptedCount();
            long rejected = h.metrics.totalRejected();
            log.info("[STEP] accepted={} rejected={} total={} dedupSize={} journalSize={}",
                    accepted, rejected, accepted + rejected, h.deduper.size(), h.journal.size());
            assertEquals(TOTAL + 1, accepted + rejected, "每条事件必须有且仅有一个结论");
            assertEquals(TOTAL + 1, h.journal.size(),
                    "事件日志必须覆盖全部已提交结论（接受+确定拒绝），无丢失无重复");

            // 2) 版本序列完整：1..CHANGES 单调唯一，无丢失无覆盖
            List<RuleChange> history = h.ruleRegistry.history();
            assertEquals(CHANGES, history.size());
            for (int i = 0; i < CHANGES; i++) {
                assertEquals(i + 1, history.get(i).version(), "版本号必须连续无空洞");
            }
            log.info("[STEP] 规则版本 1..{} 全部登记，最新 threshold={}",
                    CHANGES, h.ruleRegistry.currentRules().get(0).rule().threshold());

            // 3) 实时结论与同边界复盘结论逐位一致（跨视图：聚合 / 告警 / 调用链）
            ReplayReport replay = h.replay(log, maxT + 10_000);
            assertEquals(ReplayStatus.OK, replay.status());
            assertEquals(accepted, replay.acceptedCount(), "复盘接受数必须与实时一致");

            List<WindowAggregate> liveAgg = new ArrayList<>(h.windowStore.query(null, null));
            List<WindowAggregate> replayAgg = new ArrayList<>(replay.aggregates());
            Comparator<WindowAggregate> aggCmp = Comparator
                    .comparingLong(WindowAggregate::windowStartMillis)
                    .thenComparing(WindowAggregate::service)
                    .thenComparing(WindowAggregate::metric)
                    .thenComparing(a -> a.tags().toString());
            liveAgg.sort(aggCmp);
            replayAgg.sort(aggCmp);
            assertEquals(liveAgg, replayAgg, "聚合视图：实时定稿结果必须与复盘一致");

            List<AlertSnapshot> liveAlerts = new ArrayList<>(h.alertEngine.snapshots());
            List<AlertSnapshot> replayAlerts = new ArrayList<>(replay.alerts());
            Comparator<AlertSnapshot> alertCmp = Comparator
                    .comparing(AlertSnapshot::ruleId).thenComparing(AlertSnapshot::subjectKey);
            liveAlerts.sort(alertCmp);
            replayAlerts.sort(alertCmp);
            assertEquals(liveAlerts, replayAlerts, "告警视图：实时状态必须与复盘一致");

            List<TraceSnapshot> liveTraces = new ArrayList<>(h.traceRegistry.snapshotAll());
            List<TraceSnapshot> replayTraces = new ArrayList<>(replay.traces());
            Comparator<TraceSnapshot> traceCmp = Comparator.comparing(TraceSnapshot::traceId);
            liveTraces.sort(traceCmp);
            replayTraces.sort(traceCmp);
            assertEquals(liveTraces, replayTraces, "链路视图：实时拓扑结论必须与复盘一致");

            // 4) 每条告警的规则版本都真实存在（无撕裂状态）
            for (AlertSnapshot s : liveAlerts) {
                assertTrue(s.ruleVersion() >= 0 && s.ruleVersion() <= CHANGES,
                        "告警引用的规则版本必须已登记: " + s.ruleVersion());
            }
            log.info("[ASSERT] 并发变更+摄取下：计数守恒、版本连续、三视图实时/复盘逐位一致");
            h.dumpState(log);
        }
    }
}

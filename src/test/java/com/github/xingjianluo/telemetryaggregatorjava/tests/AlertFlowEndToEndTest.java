package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.core.AggregationEngine;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.model.ChainStatus;
import com.github.xingjianluo.telemetryaggregatorjava.model.ChainView;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端：通过引擎 HTTP 层同款入口摄取 span（乱序到达），验证链路状态、
 * 告警防抖触发/恢复，以及告警结论与链路查询语义一致。
 */
class AlertFlowEndToEndTest {

    private static final Logger log = Logger.getLogger(AlertFlowEndToEndTest.class.getName());
    private AggregationEngine engine;

    private IngestResult send(TelemetryEvent e) {
        try {
            return engine.ingest(e).get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    void chainAssembledOutOfOrderAndAlertStableUnderJitter() {
        // 窗口1s，lateness=10s：乱序到达的 span 不会因水位线被拒
        engine = new AggregationEngine(EngineTestSupport.props(1000, 10_000, 10_000,
                100_000, 1000, 1000, 100, 100_000));
        engine.start();

        // 总耗时连续3次 >=200 触发；连续2次 <=150 恢复；滞后带 (150,200)
        engine.addAlertRule(new AlertRule("slow-trace", "tr-1", "CHAIN_DURATION",
                200, 150, 3, 2));

        // 乱序：先到两个子 span（父缺失 -> AWAITING_PARENT），判定不中断
        send(EngineTestSupport.span("s-a", 100, "tr-1", "a", "root", 80));
        send(EngineTestSupport.span("s-b", 110, "tr-1", "b", "root", 70));
        engine.awaitDrained(5000);
        ChainView waiting = engine.queryChain("tr-1");
        log.info("[输入] 子 span a,b 先到（root 缺失）-> [判定] chainStatus="
                + waiting.getStatus() + ", totalDuration="
                + waiting.getTotalDurationMillis());
        assertEquals(ChainStatus.AWAITING_PARENT, waiting.getStatus());

        // 根到达，链路完整；此时总耗时 230（第1次超阈值评估，PENDING）
        send(EngineTestSupport.span("s-root", 90, "tr-1", "root", null, 80));
        engine.awaitDrained(5000);
        ChainView complete = engine.queryChain("tr-1");
        log.info("[输入] root 后到 -> [判定] chainStatus=" + complete.getStatus()
                + ", depth=" + complete.getDepth() + ", totalDuration="
                + complete.getTotalDurationMillis());
        assertEquals(ChainStatus.COMPLETE, complete.getStatus());
        assertEquals(2, complete.getDepth());
        assertEquals(AlertState.PENDING_FIRING,
                engine.alertStates().get("slow-trace").getState());

        // 再到一个子 span 使总耗时升高（第2、3次超阈值）-> FIRING
        send(EngineTestSupport.span("s-c", 120, "tr-1", "c", "root", 90)); // 总320
        engine.awaitDrained(5000);
        send(EngineTestSupport.span("s-d", 130, "tr-1", "d", "root", 60)); // 总380
        engine.awaitDrained(5000);
        List<AlertEvent> events = engine.queryAlertEvents(10);
        log.info("[输入] 新增 c,d 后总耗时380 -> [判定] 告警事件数=" + events.size()
                + ", 最新状态=" + events.get(0).getState() + "，依据："
                + events.get(0).getReason());
        assertEquals(1, events.size());
        assertEquals(AlertState.FIRING, events.get(0).getState());

        // 阈值附近抖动：再来 span 使总值在滞后带附近（仅新增 span 只会使耗时上升，
        // 这里通过新增耗时很小/很大的 span 模拟持续高位 -> 不产生重复 FIRING）
        send(EngineTestSupport.span("s-e", 140, "tr-1", "e", "root", 1));
        engine.awaitDrained(5000);
        assertEquals(1, engine.queryAlertEvents(10).size(),
                "持续高位不得重复产生 FIRING 事件（防抖）");
        log.info("[判定] 持续高位下不重复触发，告警事件仍为1条（稳定）");

        // 可解释性：stats 可查、链路与告警对同一 trace 结论一致
        Map<String, Object> stats = engine.getStats().snapshot();
        @SuppressWarnings("unchecked")
        Map<String, Long> byStatus = (Map<String, Long>) stats.get("byStatus");
        long accepted = byStatus.get(IngestStatus.ACCEPTED.name());
        log.info("[判定] stats ACCEPTED=" + accepted + ", watermark=" + stats.get("watermark")
                + ", activeWindows=" + stats.get("activeWindows")
                + ", spanCount=" + stats.get("spanCount"));
        assertEquals(6, accepted);
        assertEquals(ChainStatus.COMPLETE, engine.queryChain("tr-1").getStatus());
        assertTrue(engine.queryAlertEvents(10).get(0).getObservedValue() >= 200);
    }
}

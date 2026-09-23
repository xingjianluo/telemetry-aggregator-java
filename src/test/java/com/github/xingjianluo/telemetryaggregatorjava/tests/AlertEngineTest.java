package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.core.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.model.ChainView;
import com.github.xingjianluo.telemetryaggregatorjava.model.ChainStatus;
import com.github.xingjianluo.telemetryaggregatorjava.model.SpanNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 告警稳定性单测：连续计数 + 滞后带，阈值附近抖动不得反复触发/恢复。
 *
 * <p>滞后带语义：resolveThreshold &lt; value &lt; fireThreshold 时，
 * 两个方向的连续计数都冻结、状态保持——带内抖动既不积累触发也不打断触发积累。</p>
 */
class AlertEngineTest {

    private static final Logger log = Logger.getLogger(AlertEngineTest.class.getName());

    /** fire>=100 连续3次；resolve<=80 连续2次；滞后带 (80,100)。 */
    private AlertRule rule() {
        return new AlertRule("r1", "T", "CHAIN_DURATION", 100, 80, 3, 2);
    }

    private ChainView chain(long totalDuration) {
        SpanNode node = new SpanNode("a", null, totalDuration, 1, "e");
        return new ChainView("T", ChainStatus.COMPLETE, List.of(), List.of(node),
                totalDuration, 1, 1);
    }

    private List<AlertEvent> eval(AlertEngine engine, int seq, long duration) {
        List<AlertEvent> events = engine.evaluate("T", chain(duration), seq);
        AlertState state = engine.states().get("r1").getState();
        int streak = engine.states().get("r1").getStreak();
        log.info("[输入] 第" + seq + "次评估 value=" + duration
                + " -> [判定] state=" + state + ", streak=" + streak
                + (events.isEmpty() ? "" : ", 变迁=" + events.get(0).getReason()));
        return events;
    }

    @Test
    void jitterAroundThresholdDoesNotFlap() {
        AlertEngine engine = new AlertEngine(100);
        engine.addRule(rule());

        // 90,105,90：90 落在滞后带，计数冻结；两次带外高值之间没有“明确低值”，
        // 因此触发计数不被清零（高1 -> 带内冻结 -> 高2），但仍不足3次，不触发
        eval(engine, 1, 90);
        assertEquals(AlertState.OK, engine.states().get("r1").getState());
        eval(engine, 2, 105);
        assertEquals(AlertState.PENDING_FIRING, engine.states().get("r1").getState());
        eval(engine, 3, 90);
        assertEquals(AlertState.PENDING_FIRING, engine.states().get("r1").getState());
        assertEquals(1, engine.states().get("r1").getStreak(),
                "滞后带内触发计数冻结（不中断连续）");
        log.info("[判定] 序列 90(带内)/105(高)/90(带内)：streak 冻结为1，未触发");

        // 一次明确低值 <=80 才会清回 OK（区别于带内冻结）
        eval(engine, 4, 80);
        assertEquals(AlertState.OK, engine.states().get("r1").getState());
        assertEquals(0, engine.states().get("r1").getStreak());
        log.info("[判定] 明确低值80：触发计数清零，回到 OK");

        // 连续 3 次 >=100 触发（中间夹一个带内值不打断连续性：高-带内-高-高）
        eval(engine, 5, 100);
        eval(engine, 6, 85); // 带内冻结
        AlertState s6 = engine.states().get("r1").getState();
        int k6 = engine.states().get("r1").getStreak();
        assertEquals(AlertState.PENDING_FIRING, s6);
        assertEquals(1, k6, "带内值不打断连续触发计数");
        eval(engine, 7, 120);
        List<AlertEvent> fire = eval(engine, 8, 130);
        assertEquals(1, fire.size());
        assertEquals(AlertState.FIRING, fire.get(0).getState());
        log.info("[判定] 高/带内/高/高 连续3次高位 -> FIRING, value="
                + fire.get(0).getObservedValue());

        // 滞后带内抖动（85、95）：保持 FIRING，不产生恢复
        eval(engine, 9, 85);
        assertEquals(AlertState.FIRING, engine.states().get("r1").getState());
        eval(engine, 10, 95);
        assertEquals(AlertState.FIRING, engine.states().get("r1").getState());
        log.info("[判定] 滞后带内 85/95 抖动保持 FIRING，未恢复");

        // 低于恢复阈值但不连续（80 一次后又 100）：不恢复
        eval(engine, 11, 80);
        assertEquals(AlertState.PENDING_RESOLVED, engine.states().get("r1").getState());
        eval(engine, 12, 100);
        assertEquals(AlertState.FIRING, engine.states().get("r1").getState());
        log.info("[判定] 仅一次 <=80 后回到高值，恢复计数清零，仍 FIRING");

        // 连续 2 次 <=80 才恢复
        eval(engine, 13, 70);
        List<AlertEvent> resolved = eval(engine, 14, 60);
        assertEquals(1, resolved.size());
        assertEquals(AlertState.OK, resolved.get(0).getState());
        log.info("[判定] 连续2次 <=80 -> RESOLVED");
    }

    @Test
    void singleSpikeMustNotFire() {
        AlertEngine engine = new AlertEngine(100);
        engine.addRule(rule());
        eval(engine, 1, 200); // 单次尖峰
        assertEquals(AlertState.PENDING_FIRING, engine.states().get("r1").getState());
        eval(engine, 2, 10);
        assertEquals(AlertState.OK, engine.states().get("r1").getState());
        assertEquals(0, engine.recentEvents(10).size(), "尖峰不得产生告警事件");
        log.info("[判定] 单次尖峰200后立即回落到10 -> 无 FIRING 事件，状态机稳定");
    }
}

package com.github.xingjianluo.telemetryaggregatorjava.core;

import com.github.xingjianluo.telemetryaggregatorjava.model.AlertEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.model.ChainView;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于调用链指标的告警引擎（单线程访问）。
 *
 * <p>稳定性规则（两道防抖）：</p>
 * <ol>
 *   <li><b>连续计数</b>：观测值连续 {@code fireConsecutive} 次 &ge;
 *       {@code fireThreshold} 才 FIRING；FIRING 后连续
 *       {@code resolveConsecutive} 次 &le; {@code resolveThreshold} 才恢复 OK。
 *       计数期间任何一次不满足都清零，瞬时抖动不会触发/恢复。</li>
 *   <li><b>滞后带</b>：{@code resolveThreshold < fireThreshold} 时，落在两阈值
 *       之间的值既不推进触发也不推进恢复，阈值附近的来回波动被吸收。</li>
 * </ol>
 *
 * <p>孤立/乱序/成环链路同样参与判定（使用当前可还原出的总耗时/深度），
 * 判定不会因结构不完整而中断；结构问题通过 ChainStatus 另行标记。</p>
 */
public final class AlertEngine {

    /** 规则运行态视图。 */
    public static final class RuleStateView {
        private final AlertRule rule;
        private final AlertState state;
        private final int streak;
        private final Double lastObserved;

        public RuleStateView(AlertRule rule, AlertState state, int streak, Double lastObserved) {
            this.rule = rule;
            this.state = state;
            this.streak = streak;
            this.lastObserved = lastObserved;
        }

        public AlertRule getRule() {
            return rule;
        }

        public AlertState getState() {
            return state;
        }

        public int getStreak() {
            return streak;
        }

        public Double getLastObserved() {
            return lastObserved;
        }
    }

    /** 规则可变运行态。 */
    private static final class RuleState {
        final AlertRule rule;
        AlertState state = AlertState.OK;
        int streak = 0;
        Double lastObserved;

        RuleState(AlertRule rule) {
            this.rule = rule;
        }
    }

    private final int retainedAlertEvents;
    private final Map<String, RuleState> rules = new LinkedHashMap<>();
    private final ArrayDeque<AlertEvent> events = new ArrayDeque<>();

    public AlertEngine(int retainedAlertEvents) {
        this.retainedAlertEvents = retainedAlertEvents;
    }

    public void addRule(AlertRule rule) {
        if (rule.getResolveThreshold() > rule.getFireThreshold()) {
            throw new IllegalArgumentException(
                    "resolveThreshold must be <= fireThreshold for hysteresis");
        }
        if (rule.getFireConsecutive() < 1 || rule.getResolveConsecutive() < 1) {
            throw new IllegalArgumentException("consecutive counts must be >= 1");
        }
        rules.put(rule.getRuleId(), new RuleState(rule));
    }

    public boolean removeRule(String ruleId) {
        return rules.remove(ruleId) != null;
    }

    /**
     * 链路更新后评估所有匹配 traceId 的规则，返回本次产生的变迁事件
     * （仅 FIRING 与恢复到 OK 产生事件；PENDING_* 可通过 states() 观察）。
     */
    public List<AlertEvent> evaluate(String traceId, ChainView view, long eventTimeMillis) {
        List<AlertEvent> out = new ArrayList<>();
        for (RuleState rs : rules.values()) {
            if (!rs.rule.getTraceId().equals(traceId)) {
                continue;
            }
            double value = observeValue(rs.rule, view);
            rs.lastObserved = value;
            transition(rs, value, eventTimeMillis, out);
        }
        return out;
    }

    private double observeValue(AlertRule rule, ChainView view) {
        return switch (rule.getMetric()) {
            case "CHAIN_DURATION" -> view.getTotalDurationMillis();
            case "CHAIN_DEPTH" -> view.getDepth();
            default -> throw new IllegalArgumentException(
                    "unsupported alert metric: " + rule.getMetric());
        };
    }

    private void transition(RuleState rs, double value, long time, List<AlertEvent> out) {
        AlertRule rule = rs.rule;
        boolean high = value >= rule.getFireThreshold();
        boolean low = value <= rule.getResolveThreshold();
        // 滞后带：resolveThreshold < value < fireThreshold。
        // 带内的值既不满足触发也不满足恢复：两个方向的连续计数都冻结，状态保持，
        // 从而阈值附近的来回抖动不会打断“连续 N 次”的积累。
        boolean inHysteresisBand = !high && !low;

        if (inHysteresisBand) {
            return;
        }

        switch (rs.state) {
            case OK, PENDING_FIRING -> {
                if (high) {
                    rs.streak++;
                    if (rs.streak >= rule.getFireConsecutive()) {
                        rs.state = AlertState.FIRING;
                        rs.streak = 0;
                        emit(out, rs, value, time, true);
                    } else {
                        rs.state = AlertState.PENDING_FIRING;
                    }
                } else {
                    // 明确低于恢复阈值：尚未触发，触发计数清零，回到 OK
                    rs.state = AlertState.OK;
                    rs.streak = 0;
                }
            }
            case FIRING, PENDING_RESOLVED -> {
                if (low) {
                    rs.streak++;
                    if (rs.streak >= rule.getResolveConsecutive()) {
                        rs.state = AlertState.OK;
                        rs.streak = 0;
                        emit(out, rs, value, time, false);
                    } else {
                        rs.state = AlertState.PENDING_RESOLVED;
                    }
                } else {
                    // 明确高于触发阈值：仍告警，恢复计数清零，回到 FIRING
                    rs.state = AlertState.FIRING;
                    rs.streak = 0;
                }
            }
        }
    }

    private void emit(List<AlertEvent> out, RuleState rs, double value, long time,
                      boolean firing) {
        AlertRule rule = rs.rule;
        String reason = firing
                ? String.format(
                        "value %.2f >= fireThreshold %.2f for %d consecutive evaluations -> FIRING",
                        value, rule.getFireThreshold(), rule.getFireConsecutive())
                : String.format(
                        "value %.2f <= resolveThreshold %.2f for %d consecutive evaluations -> RESOLVED",
                        value, rule.getResolveThreshold(), rule.getResolveConsecutive());
        AlertEvent event = new AlertEvent(rule.getRuleId(), rule.getTraceId(),
                firing ? AlertState.FIRING : AlertState.OK, value, reason, time);
        out.add(event);
        events.addLast(event);
        while (events.size() > retainedAlertEvents) {
            events.pollFirst();
        }
    }

    /** 最近的变迁事件，按时间倒序（最新在前）。 */
    public List<AlertEvent> recentEvents(int limit) {
        List<AlertEvent> all = new ArrayList<>(events);
        Collections.reverse(all);
        return all.size() <= limit ? all : all.subList(0, limit);
    }

    public Map<String, RuleStateView> states() {
        Map<String, RuleStateView> out = new LinkedHashMap<>();
        rules.forEach((id, rs) -> out.put(id,
                new RuleStateView(rs.rule, rs.state, rs.streak, rs.lastObserved)));
        return out;
    }
}

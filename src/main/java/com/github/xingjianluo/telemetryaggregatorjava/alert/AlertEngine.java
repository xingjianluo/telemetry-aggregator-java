package com.github.xingjianluo.telemetryaggregatorjava.alert;

import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 告警评估引擎。
 *
 * <p>稳定性规则（抑制阈值附近抖动）：
 * <ul>
 *   <li>每个“规则 × 评估对象（标签序列或 traceId）”维护独立状态机；</li>
 *   <li>状态 OK：连续 {@code breachStreak} 个窗口超阈值才进入 FIRING（中间任一窗口
 *       未超阈值则计数清零），不会因单点毛刺立刻报警；</li>
 *   <li>状态 FIRING：连续 {@code recoverStreak} 个窗口未超阈值才回到 OK；</li>
 *   <li>PENDING 表示已观察到越界但确认次数不足，对外可见但不算报警；</li>
 *   <li>FIRING/恢复翻转还受 {@code cooldown} 冷却限制；冷却期内维持原状态；</li>
 *   <li>只有窗口定稿（关闭）才驱动评估，避免在线部分数据反复改写结论。</li>
 * </ul>
 *
 * <p>时间基准：状态机只使用<b>事件时间</b>（定稿窗口的 end）作为“现在”，
 * 不使用墙上时钟，因此同一批输入无论何时、在哪台机器上评估都得到同一结论，
 * 这也是复盘可复现的前提。
 *
 * <p>规则版本化：每次评估使用调用方按窗口结束时间解析出的 {@link ResolvedRule}
 * （规则定义 + 版本 + 免打扰边界）。免打扰区间内的窗口只记录证据、不推进状态机
 * （streak 清零），免打扰结束后需重新累计确认次数，平稳恢复、绝不补发。
 *
 * <p>调用链作用域：链路被标记为 ORPHAN/CYCLE/DUPLICATE_SPAN 时不中止判定，
 * 继续按当前数据评估并在快照中携带 traceMarker，保证结论可解释。
 */
public class AlertEngine {

    private static final class Subject {
        AlertState state = AlertState.OK;
        int consecutiveBreaches;
        int consecutiveRecoveries;
        double lastValue = Double.NaN;
        long lastWindowStart = -1L;
        /** 最近一次状态翻转发生的窗口 end（事件时间）。 */
        long lastChangedMillis = -1L;
        long ruleVersion;
        boolean muted;
        Map<String, String> tags = Map.of();
        String traceMarker;
        AlertRule.Scope scope = AlertRule.Scope.TAGS;
    }

    private record SubjectKey(String ruleId, String subject) {
    }

    private final int breachStreak;
    private final int recoverStreak;
    private final long cooldownMillis;

    private final Map<SubjectKey, Subject> subjects = new ConcurrentHashMap<>();

    public AlertEngine(int breachStreak, int recoverStreak, long cooldownMillis) {
        this.breachStreak = Math.max(1, breachStreak);
        this.recoverStreak = Math.max(1, recoverStreak);
        this.cooldownMillis = Math.max(0, cooldownMillis);
    }

    /** 标签作用域：每个关闭窗口的聚合按该窗口生效的规则评估。 */
    public void onWindowSealed(WindowAggregate aggregate, List<ResolvedRule> rules) {
        for (ResolvedRule resolved : rules) {
            AlertRule rule = resolved.rule();
            if (rule.scope() != AlertRule.Scope.TAGS) {
                continue;
            }
            if (!matches(rule, aggregate)) {
                continue;
            }
            String key = com.github.xingjianluo.telemetryaggregatorjava.window.WindowStore
                    .tagsSignature(aggregate.tags());
            Subject subject = subjects.computeIfAbsent(new SubjectKey(rule.id(), key), k -> new Subject());
            subject.scope = AlertRule.Scope.TAGS;
            subject.tags = aggregate.tags();
            evaluate(resolved, subject, aggregate.avg(),
                    aggregate.windowStartMillis(), aggregate.windowEndMillis(), null);
        }
    }

    /** 调用链作用域：由摄取管道按 trace 聚合的窗口结果驱动。 */
    public void onTraceWindowSealed(String traceId, String service, String metric,
                                    double avg, long count, long windowStartMillis,
                                    long windowEndMillis,
                                    TraceStatus traceStatus, List<String> traceReasons,
                                    List<ResolvedRule> rules) {
        for (ResolvedRule resolved : rules) {
            AlertRule rule = resolved.rule();
            if (rule.scope() != AlertRule.Scope.TRACE) {
                continue;
            }
            if (rule.service() != null && !rule.service().isBlank() && !rule.service().equals(service)) {
                continue;
            }
            if (rule.metric() != null && !rule.metric().isBlank() && !rule.metric().equals(metric)) {
                continue;
            }
            Subject subject = subjects.computeIfAbsent(new SubjectKey(rule.id(), traceId), k -> new Subject());
            subject.scope = AlertRule.Scope.TRACE;
            String marker = traceStatus == TraceStatus.COMPLETE ? null
                    : traceStatus + (traceReasons.isEmpty() ? "" : ": " + traceReasons);
            evaluate(resolved, subject, avg, windowStartMillis, windowEndMillis, marker);
        }
    }

    private boolean matches(AlertRule rule, WindowAggregate aggregate) {
        if (rule.service() != null && !rule.service().isBlank() && !rule.service().equals(aggregate.service())) {
            return false;
        }
        if (rule.metric() != null && !rule.metric().isBlank() && !rule.metric().equals(aggregate.metric())) {
            return false;
        }
        for (Map.Entry<String, String> sel : rule.tagSelector().entrySet()) {
            String actual = aggregate.tags().get(sel.getKey());
            if (!sel.getValue().equals(actual)) {
                return false;
            }
        }
        return true;
    }

    private synchronized void evaluate(ResolvedRule resolved, Subject subject, double value,
                                       long windowStartMillis, long windowEndMillis,
                                       String traceMarker) {
        subject.lastValue = value;
        subject.lastWindowStart = windowStartMillis;
        subject.ruleVersion = resolved.version();
        if (traceMarker != null) {
            subject.traceMarker = traceMarker;
        }

        // 免打扰：窗口 end 落在免打扰区间内 -> 只记录证据，不推进状态机；
        // streak 清零，免打扰结束后需按当前证据重新累计确认，平稳恢复、不补发。
        if (resolved.muteUntilMillis() > 0 && windowEndMillis <= resolved.muteUntilMillis()) {
            subject.muted = true;
            subject.consecutiveBreaches = 0;
            subject.consecutiveRecoveries = 0;
            return;
        }
        subject.muted = false;

        boolean breach = resolved.rule().breaches(value);
        // 状态机时间基准 = 定稿窗口 end（事件时间），与墙上时钟无关，保证复盘可复现。
        long now = windowEndMillis;

        if (subject.state == AlertState.FIRING) {
            if (breach) {
                subject.consecutiveBreaches++;
                subject.consecutiveRecoveries = 0;
            } else {
                subject.consecutiveRecoveries++;
                subject.consecutiveBreaches = 0;
                if (subject.consecutiveRecoveries >= recoverStreak
                        && now - subject.lastChangedMillis >= cooldownMillis) {
                    subject.state = AlertState.OK;
                    subject.lastChangedMillis = now;
                }
            }
            return;
        }

        // OK / PENDING
        if (breach) {
            subject.consecutiveBreaches++;
            subject.consecutiveRecoveries = 0;
            if (subject.consecutiveBreaches < breachStreak) {
                subject.state = AlertState.PENDING;
            } else if (subject.lastChangedMillis < 0
                    || now - subject.lastChangedMillis >= cooldownMillis) {
                subject.state = AlertState.FIRING;
                subject.lastChangedMillis = now;
            }
        } else {
            subject.consecutiveBreaches = 0;
            subject.consecutiveRecoveries = 0;
            subject.state = AlertState.OK;
        }
    }

    public synchronized List<AlertSnapshot> snapshots() {
        List<AlertSnapshot> out = new ArrayList<>();
        subjects.forEach((key, s) ->
                out.add(new AlertSnapshot(
                        key.ruleId(), s.scope, key.subject(),
                        Map.copyOf(s.tags), s.state, s.consecutiveBreaches,
                        s.consecutiveRecoveries, s.lastValue, s.lastWindowStart,
                        s.lastChangedMillis, s.traceMarker, s.ruleVersion, s.muted)));
        return out;
    }
}

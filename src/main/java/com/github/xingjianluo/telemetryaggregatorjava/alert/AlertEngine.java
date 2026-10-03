package com.github.xingjianluo.telemetryaggregatorjava.alert;

import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.replay.AlertCheckpoint;
import com.github.xingjianluo.telemetryaggregatorjava.replay.EventJournal;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersionStore;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceStatus;
import com.github.xingjianluo.telemetryaggregatorjava.window.WindowStore;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 告警评估引擎（版本化规则 + 滞回状态机 + 免打扰）。
 *
 * <p>规则来源：每个定稿窗口在评估时从 {@link RuleVersionStore} 按
 * {@code effectiveFrom < windowEnd} 解析当时生效的版本。因此：
 * 边界之前已经定稿的窗口维持原判，边界之后定稿的窗口按新版本判定；
 * 评估只由 worker 单线程驱动，规则变更也在同一线性化点登记，
 * 不会出现新旧规则互相覆盖。
 *
 * <p>滞回状态机抑制阈值附近抖动：OK 需连续 breachStreak 个越界窗口才 FIRING，
 * FIRING 需连续 recoverStreak 个恢复窗口才回 OK；翻转还受事件时间 cooldown 限制
 * （用窗口结束时间度量，保证复盘可复现——不依赖墙上时钟）。
 *
 * <p>免打扰（quiet）：与规则版本绑定的事件时间半开区间。覆盖到的窗口证据照常累计
 * （breach/recovery 计数继续走），但<b>不发生对外状态翻转</b>；区间结束后按当前证据
 * 继续状态机，每个窗口最多一次翻转，天然不会在免打扰结束时补发一串告警。
 *
 * <p>每次评估产出一条 {@link AlertFact}（有可选日志时）并在主体上保留最近依据，
 * 在线快照与按事件时间复盘因此都能逐条解释结论。
 */
public class AlertEngine {

    /** 状态机主体（规则 × 评估对象）。 */
    static final class Subject {
        AlertState state = AlertState.OK;
        int consecutiveBreaches;
        int consecutiveRecoveries;
        double lastValue = Double.NaN;
        long lastWindowStart = -1L;
        long lastWindowEnd = -1L;
        long lastChangedEventTime = -1L;
        Map<String, String> tags = Map.of();
        String traceMarker;
        AlertRule.Scope scope = AlertRule.Scope.TAGS;
        long ruleVersion;
        boolean quietLast;
        long suppressedCount;
        final Deque<String> basis = new ArrayDeque<>();
    }

    /** 主体状态的可序列化快照（用于日志老化时保存复盘下界处的状态）。 */
    public record SubjectState(
            String ruleId, AlertRule.Scope scope, String subjectKey,
            Map<String, String> tags, AlertState state,
            int consecutiveBreaches, int consecutiveRecoveries,
            double lastValue, long lastWindowStart, long lastWindowEnd,
            long lastChangedEventTime, String traceMarker,
            long ruleVersion, boolean quietLast, long suppressedCount
    ) {
    }

    private record SubjectKey(String ruleId, AlertRule.Scope scope, String subject) {
    }

    private static final int BASIS_PER_SUBJECT = 20;

    private final RuleVersionStore ruleStore;
    private final EventJournal journal;
    private final int breachStreak;
    private final int recoverStreak;
    private final long cooldownMillis;

    private final Map<SubjectKey, Subject> subjects = new ConcurrentHashMap<>();

    /** 兼容旧用法：固定规则集（内部包装为 v1、从头生效），不输出判定事实。 */
    public AlertEngine(List<AlertRule> rules, int breachStreak, int recoverStreak, long cooldownMillis) {
        this(buildStaticStore(rules), null, breachStreak, recoverStreak, cooldownMillis);
    }

    public AlertEngine(RuleVersionStore ruleStore, EventJournal journal,
                       int breachStreak, int recoverStreak, long cooldownMillis) {
        this.ruleStore = ruleStore;
        this.journal = journal;
        this.breachStreak = Math.max(1, breachStreak);
        this.recoverStreak = Math.max(1, recoverStreak);
        this.cooldownMillis = Math.max(0, cooldownMillis);
    }

    private static RuleVersionStore buildStaticStore(List<AlertRule> rules) {
        RuleVersionStore store = new RuleVersionStore();
        List<RuleVersion> seeded = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (AlertRule r : List.copyOf(rules)) {
            seeded.add(new RuleVersion(1, r.id(), true, r.scope(), r.service(), r.metric(),
                    r.tagSelector(), r.threshold(), r.operator(),
                    -1L, -1L, Long.MIN_VALUE, now, "启动配置"));
        }
        store.seed(seeded);
        return store;
    }

    /** 标签作用域：每个关闭窗口的聚合按当时生效的规则版本评估。 */
    public void onWindowSealed(WindowAggregate aggregate) {
        long wStart = aggregate.windowStartMillis();
        long wEnd = aggregate.windowEndMillis();
        for (String ruleId : ruleStore.ruleIds()) {
            RuleVersion version = ruleStore.versionAt(ruleId, wEnd);
            if (version == null || !version.enabled() || version.scope() != AlertRule.Scope.TAGS) {
                continue;
            }
            AlertRule rule = version.toAlertRule();
            if (!matches(rule, aggregate)) {
                continue;
            }
            String key = WindowStore.tagsSignature(aggregate.tags());
            Subject subject = subjects.computeIfAbsent(
                    new SubjectKey(ruleId, AlertRule.Scope.TAGS, key), k -> new Subject());
            subject.scope = AlertRule.Scope.TAGS;
            subject.tags = aggregate.tags();
            evaluate(version, subject, aggregate.avg(), wStart, wEnd, null, key);
        }
    }

    /** 调用链作用域：由摄取管道按 trace 聚合的窗口结果驱动。 */
    public void onTraceWindowSealed(String traceId, String service, String metric,
                                    double avg, long count, long windowStartMillis,
                                    long windowEndMillis,
                                    TraceStatus traceStatus, List<String> traceReasons) {
        for (String ruleId : ruleStore.ruleIds()) {
            RuleVersion version = ruleStore.versionAt(ruleId, windowEndMillis);
            if (version == null || !version.enabled() || version.scope() != AlertRule.Scope.TRACE) {
                continue;
            }
            AlertRule rule = version.toAlertRule();
            if (rule.service() != null && !rule.service().isBlank() && !rule.service().equals(service)) {
                continue;
            }
            if (rule.metric() != null && !rule.metric().isBlank() && !rule.metric().equals(metric)) {
                continue;
            }
            Subject subject = subjects.computeIfAbsent(
                    new SubjectKey(ruleId, AlertRule.Scope.TRACE, traceId), k -> new Subject());
            subject.scope = AlertRule.Scope.TRACE;
            String marker = traceStatus == TraceStatus.COMPLETE ? null
                    : traceStatus + (traceReasons.isEmpty() ? "" : ": " + traceReasons);
            evaluate(version, subject, avg, windowStartMillis, windowEndMillis, marker, traceId);
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

    /**
     * 状态机单步。worker 单线程调用；复盘在单线程重放中以完全相同的代码路径调用，
     * 因此实时与复盘结论一致且可复现。
     */
    private void evaluate(RuleVersion version, Subject subject, double value,
                          long wStart, long wEnd, String traceMarker, String displayKey) {
        AlertRule rule = version.toAlertRule();
        AlertState from = subject.state;
        subject.lastValue = value;
        subject.lastWindowStart = wStart;
        subject.lastWindowEnd = wEnd;
        subject.ruleVersion = version.version();
        if (traceMarker != null) {
            subject.traceMarker = traceMarker;
        }
        boolean breach = rule.breaches(value);
        boolean quiet = version.quietOverlapsWindow(wStart, wEnd);
        subject.quietLast = quiet;
        boolean cooldownOk = subject.lastChangedEventTime < 0
                || wEnd - subject.lastChangedEventTime >= cooldownMillis;

        AlertState to = from;
        String transitionNote;

        if (from == AlertState.FIRING) {
            if (breach) {
                subject.consecutiveBreaches++;
                subject.consecutiveRecoveries = 0;
                transitionNote = "仍越界，breachStreak=" + subject.consecutiveBreaches;
            } else {
                subject.consecutiveRecoveries++;
                subject.consecutiveBreaches = 0;
                if (subject.consecutiveRecoveries >= recoverStreak && cooldownOk && !quiet) {
                    to = AlertState.OK;
                    subject.lastChangedEventTime = wEnd;
                    transitionNote = "连续恢复 " + subject.consecutiveRecoveries
                            + " 窗口且冷却已过 => FIRING->OK";
                } else if (quiet) {
                    subject.suppressedCount++;
                    transitionNote = "恢复证据累计 recoverStreak=" + subject.consecutiveRecoveries
                            + "，但窗口落在免打扰区间，状态维持 FIRING（不补发）";
                } else if (!cooldownOk) {
                    transitionNote = "恢复计数满足但冷却未过（cooldown=" + cooldownMillis
                            + "ms），维持 FIRING";
                } else {
                    transitionNote = "恢复中 recoverStreak=" + subject.consecutiveRecoveries
                            + "/" + recoverStreak + "，维持 FIRING";
                }
            }
        } else {
            // OK / PENDING
            if (breach) {
                subject.consecutiveBreaches++;
                subject.consecutiveRecoveries = 0;
                if (subject.consecutiveBreaches >= breachStreak && cooldownOk && !quiet) {
                    to = AlertState.FIRING;
                    subject.lastChangedEventTime = wEnd;
                    transitionNote = "连续越界 " + subject.consecutiveBreaches
                            + " 窗口且冷却已过 => " + from + "->FIRING";
                } else if (quiet) {
                    // 免打扰：证据累计但对外状态维持原样（OK 仍是 OK，不提前变 PENDING/FIRING）。
                    to = from;
                    subject.suppressedCount++;
                    transitionNote = "越界证据累计 breachStreak=" + subject.consecutiveBreaches
                            + "/" + breachStreak + "，窗口落在免打扰区间，抑制对外状态翻转";
                } else if (subject.consecutiveBreaches < breachStreak) {
                    to = AlertState.PENDING;
                    transitionNote = "越界 breachStreak=" + subject.consecutiveBreaches
                            + "/" + breachStreak + " => PENDING（单点毛刺不报警）";
                } else {
                    transitionNote = "越界确认但冷却未过（cooldown=" + cooldownMillis
                            + "ms），维持 " + from;
                }
            } else {
                subject.consecutiveBreaches = 0;
                subject.consecutiveRecoveries = 0;
                to = AlertState.OK;
                transitionNote = from == AlertState.PENDING
                        ? "正常窗口清零毛刺计数 => PENDING->OK" : "未越界，保持 OK";
            }
        }
        subject.state = to;

        String reason = "window=[" + wStart + "," + wEnd + ") rule=" + version.id()
                + "@v" + version.version() + " value=" + value + " " + rule.operator()
                + " " + rule.threshold() + " breach=" + breach
                + (quiet ? " quiet=suppressed" : "") + " => " + from + "->" + to
                + " | " + transitionNote;
        addBasis(subject, reason);
        emitFact(version, subject, displayKey, value, wStart, wEnd, breach, from, to, quiet, reason);
    }

    private void addBasis(Subject subject, String reason) {
        subject.basis.addLast(reason);
        while (subject.basis.size() > BASIS_PER_SUBJECT) {
            subject.basis.pollFirst();
        }
    }

    private void emitFact(RuleVersion version, Subject subject, String subjectKey, double value,
                          long wStart, long wEnd, boolean breach,
                          AlertState from, AlertState to, boolean quiet, String reason) {
        if (journal == null) {
            return;
        }
        long seq = journal.nextSeq();
        AlertFact fact = new AlertFact(seq, wStart, wEnd, version.id(), version.version(),
                subject.scope, subjectKey,
                Map.copyOf(subject.tags), value, breach, from, to, quiet,
                subject.traceMarker, List.of(reason));
        journal.appendAlertFact(fact);
    }

    /** 在日志老化（可复盘下界推进）时导出全部主体状态。 */
    public synchronized AlertCheckpoint checkpoint(long watermarkMillis) {
        List<SubjectState> states = new ArrayList<>();
        subjects.forEach((k, s) -> states.add(new SubjectState(
                k.ruleId(), k.scope(), k.subject(), Map.copyOf(s.tags), s.state,
                s.consecutiveBreaches, s.consecutiveRecoveries, s.lastValue,
                s.lastWindowStart, s.lastWindowEnd, s.lastChangedEventTime,
                s.traceMarker, s.ruleVersion, s.quietLast, s.suppressedCount)));
        return new AlertCheckpoint(watermarkMillis, List.copyOf(states));
    }

    /** 复盘重放前恢复下界处的主体状态（只在全新引擎实例上调用一次）。 */
    public synchronized void restore(List<SubjectState> states) {
        subjects.clear();
        for (SubjectState st : states) {
            SubjectKey key = new SubjectKey(st.ruleId(), st.scope(), st.subjectKey());
            Subject s = new Subject();
            s.tags = st.tags();
            s.state = st.state();
            s.consecutiveBreaches = st.consecutiveBreaches();
            s.consecutiveRecoveries = st.consecutiveRecoveries();
            s.lastValue = st.lastValue();
            s.lastWindowStart = st.lastWindowStart();
            s.lastWindowEnd = st.lastWindowEnd();
            s.lastChangedEventTime = st.lastChangedEventTime();
            s.traceMarker = st.traceMarker();
            s.ruleVersion = st.ruleVersion();
            s.quietLast = st.quietLast();
            s.suppressedCount = st.suppressedCount();
            subjects.put(key, s);
        }
    }

    public synchronized List<AlertSnapshot> snapshots() {
        List<AlertSnapshot> out = new ArrayList<>();
        subjects.forEach((key, s) -> out.add(new AlertSnapshot(
                key.ruleId(), s.scope, key.subject(),
                Map.copyOf(s.tags), s.state, s.consecutiveBreaches,
                s.consecutiveRecoveries, s.lastValue, s.lastWindowStart,
                s.lastChangedEventTime, s.traceMarker,
                s.ruleVersion, s.quietLast, s.suppressedCount,
                List.copyOf(s.basis))));
        return out;
    }
}

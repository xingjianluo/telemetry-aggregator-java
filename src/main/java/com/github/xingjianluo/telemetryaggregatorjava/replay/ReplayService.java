package com.github.xingjianluo.telemetryaggregatorjava.replay;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersionStore;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.window.WatermarkGenerator;
import com.github.xingjianluo.telemetryaggregatorjava.window.WindowStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 按事件时间复盘：给定时间点 T，纯只读重放日志，给出那一刻已定稿的窗口聚合、
 * 当时生效的规则版本、告警状态与调用链结论，并逐条说明依据。
 *
 * <p><b>确定性</b>：重放只依赖日志里的输入事件（携带线性化 seq 与处理后水位线）
 * 和规则变更历史；没有墙上时钟、没有随机、没有空闲回收，使用与实时完全相同的
 * 计入 / 定稿 / 状态机代码路径。同一批输入无论何时执行、在哪台机器执行，结论一致。
 *
 * <p><b>切点语义</b>：先找到“处理后水位线首次 ≥ T”的事件 seq（cutoff）；
 * 对 ≤cutoff 的事件做重放，并以该事件处理后的水位线关闭窗口。这与实时系统
 * 在该线性化点对外可见的结论严格一致（不凭空关闭未来窗口，也不漏掉已关闭窗口）。
 *
 * <p><b>只读</b>：使用全新的本地 WatermarkGenerator / WindowStore / TraceRegistry /
 * RuleVersionStore / AlertEngine，绝不触碰实时状态。
 *
 * <p><b>资源边界</b>：信号量限制并发复盘数；单次扫描事件数有上限；
 * 不与摄取线程争抢（摄取状态完全不读不写）。超限快速返回 RESOURCE_LIMITED。
 *
 * <p><b>可复盘范围</b>：早于日志保留下界（最老保留事件，或存在老化时的检查点水位线）
 * 的请求返回 NOT_REPLAYABLE，并说明原因，不返回缺数据的“完整”答案。
 */
public class ReplayService {

    private final EventJournal journal;
    private final RuleVersionStore liveRuleStore;
    private final TelemetryProperties props;
    private final Semaphore concurrencySlots;
    private final AtomicInteger active = new AtomicInteger(0);

    public ReplayService(EventJournal journal, RuleVersionStore liveRuleStore,
                         TelemetryProperties props) {
        this.journal = journal;
        this.liveRuleStore = liveRuleStore;
        this.props = props;
        this.concurrencySlots = new Semaphore(
                Math.max(1, props.getReplay().getMaxConcurrentReplays()), true);
    }

    public ReplayReport replayAt(long eventTimeMillis, String traceId) {
        return replayAt(eventTimeMillis, traceId, null);
    }

    /**
     * 按事件时间点复盘。
     *
     * @param eventTimeMillis 时间点 T
     * @param traceId         非空时只返回该调用链
     * @param serviceMetric   可选的聚合过滤（service,metric），null 不过滤
     */
    public ReplayReport replayAt(long eventTimeMillis, String traceId, ServiceMetric serviceMetric) {
        if (!concurrencySlots.tryAcquire()) {
            return limited(eventTimeMillis, "并发复盘数已达上限 "
                    + props.getReplay().getMaxConcurrentReplays() + "，为保护实时摄取拒绝本次复盘");
        }
        active.incrementAndGet();
        try {
            return doReplay(eventTimeMillis, traceId, serviceMetric);
        } finally {
            active.decrementAndGet();
            concurrencySlots.release();
        }
    }

    private ReplayReport doReplay(long T, String traceId, ServiceMetric serviceMetric) {
        List<JournaledEvent> allEvents;
        List<JournaledRuleChange> changes;
        AlertCheckpoint checkpoint;
        synchronized (journal) {
            allEvents = journal.events();
            changes = journal.ruleChanges();
            checkpoint = journal.checkpoint();
        }

        // 1) 可复盘范围判定。
        long horizon;
        String unavailable = null;
        if (allEvents.isEmpty()) {
            return new ReplayReport(ReplayStatus.NOT_REPLAYABLE,
                    "尚无任何已接受事件，没有可复盘的输入", T, WatermarkGenerator.none(),
                    -1L, WatermarkGenerator.none(), 0, journal.maxEvents(),
                    List.of(), List.of(), List.of(), List.of(),
                    List.of("日志为空：复盘结论必须只由输入事件与规则变更决定"));
        }
        if (checkpoint != null) {
            horizon = checkpoint.watermarkMillis();
            if (T < horizon) {
                unavailable = "请求时间 " + T + " 早于可复盘下界 " + horizon
                        + "（更早期的事件日志已按容量老化，且无对应检查点之前的状态）";
            }
        } else {
            horizon = allEvents.get(0).event().eventTimeMillis();
            // 没有老化时，理论上最老事件的窗口也能重建；不限制 T >= 某水位线。
        }
        if (unavailable != null) {
            return new ReplayReport(ReplayStatus.NOT_REPLAYABLE, unavailable, T,
                    WatermarkGenerator.none(), -1L, horizon, 0, journal.maxEvents(),
                    List.of(), List.of(), List.of(), List.of(),
                    List.of("拒绝给出看似完整但实际缺数据的答案"));
        }

        // 2) 切点：处理后水位线首次 >= T 的事件 seq。
        long cutoffSeq = -1L;
        long cutoffWatermark = WatermarkGenerator.none();
        for (JournaledEvent je : allEvents) {
            if (je.watermarkAfterMillis() >= T) {
                cutoffSeq = je.seq();
                cutoffWatermark = je.watermarkAfterMillis();
                break;
            }
        }
        List<JournaledEvent> included;
        if (cutoffSeq < 0) {
            // T 尚未被任何输入越过：全部保留事件都参与，水位线取最后事件处理后的值，
            // 但不按 T 关闭窗口（PARTIAL）。
            included = allEvents;
            cutoffSeq = allEvents.get(allEvents.size() - 1).seq();
            cutoffWatermark = allEvents.get(allEvents.size() - 1).watermarkAfterMillis();
        } else {
            included = new ArrayList<>();
            for (JournaledEvent je : allEvents) {
                if (je.seq() <= cutoffSeq) {
                    included.add(je);
                }
            }
        }
        if (included.size() > props.getReplay().getMaxScanEventsPerReplay()) {
            return limited(T, "本次复盘需扫描 " + included.size() + " 条事件，超过单次上限 "
                    + props.getReplay().getMaxScanEventsPerReplay());
        }

        // 3) 构建截至切点的规则版本存储：按原版本号登记（初始版本 + seq<=cutoff 的变更），
        //    不重新编号，保证复盘解析的版本号与告警事实中记录的版本号完全一致。
        RuleVersionStore replayRules = new RuleVersionStore();
        List<String> changeBasis = new ArrayList<>();
        for (RuleVersion v : liveRuleStore.history()) {
            if (v.effectiveFromMillis() == Long.MIN_VALUE) {
                replayRules.registerExisting(v);
            }
        }
        for (JournaledRuleChange jc : changes) {
            if (jc.seq() > cutoffSeq) {
                break;
            }
            RuleVersion v = jc.version();
            replayRules.registerExisting(v);
            changeBasis.add("seq=" + jc.seq() + " rule=" + v.id() + "@v" + v.version()
                    + " effectiveFrom=" + v.effectiveFromMillis() + " (" + v.note() + ")");
        }

        // 4) 本地组件：全新实例，绝不触碰实时状态。
        long size = props.getWindow().getSizeMillis();
        long lateness = props.getWindow().getAllowedLatenessMillis();
        WatermarkGenerator rw = new WatermarkGenerator(size, lateness, false,
                java.time.Clock.fixed(java.time.Instant.EPOCH, java.time.ZoneOffset.UTC));
        WindowStore rws = new WindowStore(
                props.getCardinality().getMaxSeriesPerWindow(),
                Integer.MAX_VALUE);
        TraceRegistry rtr = new TraceRegistry(
                props.getCardinality().getMaxTraces(),
                props.getCardinality().getMaxSpansPerTrace());
        AlertEngine rae = new AlertEngine(replayRules, null,
                props.getAlert().getBreachStreak(),
                props.getAlert().getRecoverStreak(),
                props.getAlert().getCooldown().toMillis());
        if (checkpoint != null) {
            rae.restore(checkpoint.subjects());
        }

        long scanned = 0;
        List<String> traceIds = new ArrayList<>();
        for (JournaledEvent je : included) {
            TelemetryEvent e = je.event();
            scanned++;
            long wStart = WatermarkGenerator.windowStart(e.eventTimeMillis(), size);
            rw.observe(e.eventTimeMillis());
            rws.add(e, wStart);
            rws.addTrace(e, wStart, props.getCardinality().getMaxTraceSeriesPerWindow());
            // included 已按 seq<=cutoff 过滤：只重建 T 时刻链路可见的 span，
            // T 之后才到达的 span 不会被算进来。
            rtr.add(e);
            traceIds.add(e.traceId());
            // 与实时相同：每处理完一条事件就关闭到期窗口并评估告警。
            sealAndEvaluate(rw, rws, rtr, rae, size);
        }
        // 若切点事件把水位线推进到 T（>= 某窗口 end），上面逐事件 seal 已覆盖；
        // 若 T 恰好未触发新关闭，则无额外窗口可定稿（PARTIAL）。

        // 5) 组装结论。
        List<WindowAggregate> aggregates = rws.query(
                serviceMetric == null ? null : serviceMetric.service(),
                serviceMetric == null ? null : serviceMetric.metric());
        List<TraceSnapshot> traces = collectTraces(rtr, traceIds, traceId);
        List<RuleVersion> effective = replayRules.latestAsOf(
                cutoffSeq < 0 ? cutoffWatermark : T);
        List<AlertSnapshot> snaps = rae.snapshots();
        List<ReplayAlert> alerts = new ArrayList<>();
        for (AlertSnapshot s : snaps) {
            List<RuleVersion> hv = replayRules.historyOf(s.ruleId());
            RuleVersion atT = null;
            for (RuleVersion v : hv) {
                if (v.effectiveFromMillis() <= T) {
                    atT = v;
                }
            }
            boolean quietAtCutoff = atT != null && atT.quietAt(T);
            alerts.add(new ReplayAlert(s.ruleId(), s.ruleVersion(), s.scope().name(),
                    s.subjectKey(), s.state(), s.consecutiveBreaches(), s.consecutiveRecoveries(),
                    s.lastValue(), s.lastEvaluatedWindowStart(), s.traceMarker(),
                    quietAtCutoff, s.suppressedCount(), s.basis()));
        }

        List<String> basis = new ArrayList<>();
        basis.add("cutoff: 首个处理后水位线>=" + T + " 的线性化点 seq=" + cutoffSeq
                + "，cutoffWatermark=" + cutoffWatermark);
        basis.add("扫描事件数=" + scanned + "（重放顺序=日志线性化 seq 顺序，无墙上时钟参与）");
        basis.addAll(changeBasis);
        basis.add("窗口仅在 watermark>=windowEnd 时定稿；复盘不复用实时内存中的聚合/告警对象");

        ReplayStatus status;
        boolean reached = false;
        for (JournaledEvent je : allEvents) {
            if (je.watermarkAfterMillis() >= T) {
                reached = true;
                break;
            }
        }
        status = reached ? ReplayStatus.COMPLETE : ReplayStatus.PARTIAL;

        return new ReplayReport(status, null, T, cutoffWatermark, cutoffSeq, horizon,
                scanned, journal.maxEvents(), aggregates, effective, alerts, traces, basis);
    }

    private static void sealAndEvaluate(WatermarkGenerator rw, WindowStore rws,
                                        TraceRegistry rtr, AlertEngine rae, long size) {
        List<WindowAggregate> sealed = rws.sealUpTo(rw.currentWatermark(), size);
        for (WindowAggregate agg : sealed) {
            rae.onWindowSealed(agg);
            for (WindowStore.TraceAggregate ta : rws.sealedTraceAggregates(agg.windowStartMillis())) {
                TraceSnapshot snap = rtr.snapshot(ta.traceId());
                rae.onTraceWindowSealed(ta.traceId(), ta.service(), ta.metric(),
                        ta.avg(), ta.count(), ta.windowStartMillis(), ta.windowEndMillis(),
                        snap == null ? null : snap.status(),
                        snap == null ? List.of() : snap.reasons());
            }
        }
    }

    private List<TraceSnapshot> collectTraces(TraceRegistry rtr, List<String> traceIdsInOrder,
                                              String onlyTraceId) {
        List<TraceSnapshot> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        int cap = props.getReplay().getMaxTracesPerReport();
        for (String tid : traceIdsInOrder) {
            if (tid == null || !seen.add(tid)) {
                continue;
            }
            if (onlyTraceId != null && !onlyTraceId.isBlank() && !onlyTraceId.equals(tid)) {
                continue;
            }
            TraceSnapshot snap = rtr.snapshot(tid);
            if (snap != null) {
                out.add(snap);
                if (out.size() >= cap) {
                    break;
                }
            }
        }
        return List.copyOf(out);
    }

    private ReplayReport limited(long T, String reason) {
        return new ReplayReport(ReplayStatus.RESOURCE_LIMITED, reason, T,
                WatermarkGenerator.none(), -1L, WatermarkGenerator.none(), 0,
                journal.maxEvents(), List.of(), List.of(), List.of(), List.of(),
                List.of("资源边界：复盘不能拖慢或拖垮实时摄取"));
    }

    public long horizonMillis() {
        AlertCheckpoint cp;
        List<JournaledEvent> events;
        synchronized (journal) {
            cp = journal.checkpoint();
            events = journal.events();
        }
        if (cp != null) {
            return cp.watermarkMillis();
        }
        return events.isEmpty() ? WatermarkGenerator.none()
                : events.get(0).event().eventTimeMillis();
    }

    public int activeReplays() {
        return active.get();
    }

    /** 聚合过滤参数。 */
    public record ServiceMetric(String service, String metric) {
    }
}

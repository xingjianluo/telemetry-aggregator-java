package com.github.xingjianluo.telemetryaggregatorjava.ingest;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.alert.ResolvedRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.dedup.EventDeduper;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.replay.EventJournal;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.window.WatermarkGenerator;
import com.github.xingjianluo.telemetryaggregatorjava.window.WindowStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 确定性事件处理核心：实时摄取 worker 与复盘重放共用同一份
 * 逐事件处理、窗口定稿与告警评估逻辑，保证两侧结论逐位一致。
 *
 * <p>必须由单一线程调用（实时：ingest worker；复盘：复盘执行器），
 * 全部状态变更在此线程内线性化。规则变更同样经由
 * {@link #applyRuleChange} 在此线程内应用：先把所有可定稿窗口按旧规则定稿，
 * 再以当前水位线作为生效边界登记变更，因此“边界之前维持原判、边界之后按新规则”
 * 与事件写入不会发生交错覆盖。
 *
 * <p>每条已提交结论（接受或确定性拒绝）都会写入事件日志（若配置），
 * 作为复盘的唯一事件来源；复盘实例以 {@code journal == null} 构造，自身不再记录。
 */
public class EventProcessor {

    private final TelemetryProperties props;
    private final WatermarkGenerator watermark;
    private final WindowStore windowStore;
    private final TraceRegistry traceRegistry;
    private final AlertEngine alertEngine;
    private final RuleRegistry ruleRegistry;
    private final EventDeduper deduper;
    private final IngestionMetrics metrics;
    private final EventJournal journal;
    private final int maxTraceSeriesPerWindow;

    public EventProcessor(TelemetryProperties props,
                          WatermarkGenerator watermark, WindowStore windowStore,
                          TraceRegistry traceRegistry, AlertEngine alertEngine,
                          RuleRegistry ruleRegistry, EventDeduper deduper,
                          IngestionMetrics metrics, EventJournal journal) {
        this.props = props;
        this.watermark = watermark;
        this.windowStore = windowStore;
        this.traceRegistry = traceRegistry;
        this.alertEngine = alertEngine;
        this.ruleRegistry = ruleRegistry;
        this.deduper = deduper;
        this.metrics = metrics;
        this.journal = journal;
        this.maxTraceSeriesPerWindow = props.getCardinality().getMaxTraceSeriesPerWindow();
    }

    /** 逐事件处理（单线程调用）：幂等判定 → 迟到判定 → 计数 → 定稿 → 告警。 */
    public IngestResult process(TelemetryEvent event) {
        // 幂等判定与首次结论登记在同一线程内顺序发生：重复/冲突结论确定，永不双计数。
        EventDeduper.CheckResult check = deduper.check(event);
        if (check.conflicting()) {
            metrics.incrementRejected(RejectReason.CONFLICTING_EVENT_ID);
            return IngestResult.rejected(event.eventId(), RejectReason.CONFLICTING_EVENT_ID,
                    "相同 eventId 的载荷与首次投递不一致");
        }
        if (check.verdict() != null) {
            metrics.incrementDuplicateReplay();
            if (!check.verdict().accepted()) {
                metrics.incrementRejected(check.verdict().rejectReason());
            }
            return check.verdict();
        }

        long wmBefore = watermark.currentWatermark();

        // 迟到 / 窗口已关闭（确定拒绝，幂等登记，重试永远得到同一结论）
        if (WatermarkGenerator.behindLatenessBound(event.eventTimeMillis(),
                wmBefore, props.getWindow().getAllowedLatenessMillis())) {
            return rejectAndCommit(event, RejectReason.LATE_BEHIND_WATERMARK,
                    "eventTime=" + event.eventTimeMillis()
                            + " 早于迟到边界 watermark-allowedLateness="
                            + (wmBefore - props.getWindow().getAllowedLatenessMillis()));
        }
        long windowStart = WatermarkGenerator.windowStart(
                event.eventTimeMillis(), props.getWindow().getSizeMillis());
        if (WatermarkGenerator.windowAlreadySealed(event.eventTimeMillis(),
                props.getWindow().getSizeMillis(), wmBefore)) {
            return rejectAndCommit(event, RejectReason.LATE_BEHIND_WATERMARK,
                    "所属窗口 [" + windowStart + ","
                            + (windowStart + props.getWindow().getSizeMillis())
                            + ") 已按水位线 " + wmBefore + " 定稿关闭，迟到事件不静默计入");
        }

        // 标签形状基数检查：格式错误不登记幂等表、不占用 ID 池、不写日志：
        // 同一 ID 修正载荷后应能被接受。
        IngestResult tagReject = checkTagShape(event);
        if (tagReject != null) {
            metrics.incrementRejected(RejectReason.TAG_CARDINALITY_LIMIT);
            return tagReject;
        }

        // 去重表基数：必须在产生任何计数之前预留登记名额
        if (deduper.size() >= props.getCardinality().getMaxEventIds()) {
            return rejectAndCommit(event, RejectReason.TOO_MANY_EVENT_IDS,
                    "事件 ID 去重池已达上限 "
                            + props.getCardinality().getMaxEventIds());
        }

        // 计入窗口标签聚合
        java.util.Optional<RejectReason> r1 = windowStore.add(event, windowStart);
        if (r1.isPresent()) {
            return rejectAndCommit(event, r1.get(),
                    "窗口 " + windowStart + " 内标签序列数已达上限 "
                            + props.getCardinality().getMaxSeriesPerWindow());
        }
        // 计入 trace × 窗口聚合；失败则补偿回滚标签计数
        java.util.Optional<RejectReason> r2 =
                windowStore.addTrace(event, windowStart, maxTraceSeriesPerWindow);
        if (r2.isPresent()) {
            windowStore.removeLast(event, windowStart);
            return rejectAndCommit(event, r2.get(),
                    "窗口 " + windowStart + " 内 trace 序列数已达上限 "
                            + maxTraceSeriesPerWindow);
        }
        // 调用链拓扑登记；失败则补偿前两步
        java.util.Optional<RejectReason> r3 = traceRegistry.add(event);
        if (r3.isPresent()) {
            windowStore.removeLast(event, windowStart);
            windowStore.removeLastTrace(event, windowStart);
            return rejectAndCommit(event, r3.get(),
                    "调用链基数超限（maxTraces="
                            + props.getCardinality().getMaxTraces()
                            + ", maxSpansPerTrace="
                            + props.getCardinality().getMaxSpansPerTrace() + "）");
        }

        // 全部计入成功：登记幂等首次结论、写事件日志、推进水位线、关闭到期窗口、驱动告警
        IngestResult accepted = IngestResult.accepted(event.eventId(), windowStart);
        boolean committed = deduper.commit(event, accepted);
        if (!committed) {
            windowStore.removeLast(event, windowStart);
            windowStore.removeLastTrace(event, windowStart);
            return rejectAndCommit(event, RejectReason.TOO_MANY_EVENT_IDS,
                    "事件 ID 去重池已满，提交幂等登记失败");
        }
        if (journal != null) {
            journal.append(event, accepted);
        }
        metrics.incrementAccepted();

        watermark.observe(event.eventTimeMillis());
        sealWindowsAndEvaluate();
        return accepted;
    }

    /** 定稿所有可定稿窗口，并用各窗口生效的规则版本驱动告警评估。 */
    public void sealWindowsAndEvaluate() {
        long wm = watermark.currentWatermark();
        List<WindowAggregate> sealed =
                windowStore.sealUpTo(wm, props.getWindow().getSizeMillis());
        // 按窗口分组：同一窗口的全部序列共用同一份“该窗口生效的规则版本”。
        Map<Long, List<WindowAggregate>> byWindow = new TreeMap<>();
        for (WindowAggregate aggregate : sealed) {
            byWindow.computeIfAbsent(aggregate.windowStartMillis(), k -> new ArrayList<>())
                    .add(aggregate);
        }
        byWindow.forEach((windowStart, aggregates) -> {
            long windowEnd = WatermarkGenerator.windowEnd(
                    windowStart, props.getWindow().getSizeMillis());
            List<ResolvedRule> rules = ruleRegistry.rulesForWindow(windowEnd);
            for (WindowAggregate aggregate : aggregates) {
                alertEngine.onWindowSealed(aggregate, rules);
            }
            // trace 作用域告警：用同一批定稿窗口数据，按 trace 评估并附带拓扑标记
            for (WindowStore.TraceAggregate ta : windowStore.sealedTraceAggregates(windowStart)) {
                TraceSnapshot snapshot = traceRegistry.snapshot(ta.traceId());
                alertEngine.onTraceWindowSealed(
                        ta.traceId(), ta.service(), ta.metric(), ta.avg(), ta.count(),
                        ta.windowStartMillis(), windowEnd,
                        snapshot == null ? null : snapshot.status(),
                        snapshot == null ? List.of() : snapshot.reasons(),
                        rules);
            }
        });
        // 老化去重表：只有事件时间 < wm - size - lateness 的条目才可回收。
        long size = props.getWindow().getSizeMillis();
        long lateness = props.getWindow().getAllowedLatenessMillis();
        deduper.prune(wm - size - lateness);
    }

    /**
     * 应用规则变更（与事件处理同一线性化点）：
     * 先把所有可定稿窗口按当前（旧）规则定稿，再以当前水位线为生效边界登记变更。
     * 因此 end <= 边界的窗口永远维持原判，end > 边界的窗口按新规则判定。
     * 携带 requestId 的请求重复到达时直接返回首次登记的变更记录，不产生新版本。
     */
    public RuleChange applyRuleChange(RuleChangeRequest request) {
        sealWindowsAndEvaluate();
        return ruleRegistry.applyChange(request, watermark.currentWatermark(),
                System.currentTimeMillis());
    }

    /** 入队前的快速校验（权威校验仍在 worker 线程内重做）。 */
    public void validateRuleChange(RuleChangeRequest request) {
        ruleRegistry.validateChange(request);
    }

    /** 周期推进：空闲水位线推进 + 定稿 + 调用链回收。 */
    public void onTick() {
        long before = watermark.currentWatermark();
        long after = watermark.tick();
        if (after != before) {
            sealWindowsAndEvaluate();
        }
        traceRegistry.pruneIdle(System.currentTimeMillis(),
                props.getTrace().getRetention().toMillis());
    }

    private IngestResult rejectAndCommit(TelemetryEvent event, RejectReason reason, String detail) {
        metrics.incrementRejected(reason);
        IngestResult result = IngestResult.rejected(event.eventId(), reason, detail);
        // 确定性拒绝也登记首次结论：后续相同 ID 重试直接复用，保证可复现、不计数
        deduper.commit(event, result);
        if (journal != null) {
            journal.append(event, result);
        }
        return result;
    }

    private IngestResult checkTagShape(TelemetryEvent event) {
        Map<String, String> tags = event.tags();
        if (tags == null) {
            return null;
        }
        int maxTags = props.getCardinality().getMaxTagsPerEvent();
        int maxLen = props.getCardinality().getMaxTagValueLength();
        if (tags.size() > maxTags) {
            return IngestResult.rejected(event.eventId(), RejectReason.TAG_CARDINALITY_LIMIT,
                    "标签数 " + tags.size() + " 超过上限 " + maxTags);
        }
        for (Map.Entry<String, String> e : tags.entrySet()) {
            String k = e.getKey() == null ? "" : e.getKey();
            String v = e.getValue() == null ? "" : e.getValue();
            if (k.length() > maxLen || v.length() > maxLen) {
                return IngestResult.rejected(event.eventId(), RejectReason.TAG_CARDINALITY_LIMIT,
                        "标签 " + k + " 的 key/value 长度超过上限 " + maxLen);
            }
        }
        return null;
    }
}

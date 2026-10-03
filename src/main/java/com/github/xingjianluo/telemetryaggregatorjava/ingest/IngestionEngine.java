package com.github.xingjianluo.telemetryaggregatorjava.ingest;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.dedup.EventDeduper;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.replay.EventJournal;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeResult;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeStatus;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleManager;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersionStore;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.window.WatermarkGenerator;
import com.github.xingjianluo.telemetryaggregatorjava.window.WindowStore;
import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 摄取与规则运营的统一线性化管道。
 *
 * <p>并发模型：调用方线程只做字段校验，随后进入<b>同一个有界队列</b>；单一
 * {@code telemetry-ingest-worker} 线程顺序完成：幂等判定 → 规则变更/事件计入
 * → 水位线推进 → 窗口定稿 → 告警评估 → 复盘日志追加。事件写入与规则变更因此
 * 不可能互相覆盖、丢失状态或重复计数，每条结论的先后边界都由全局序列号固定。
 *
 * <p>规则变更的生效边界：worker 处理变更时取当前水位线（事件时间）作为
 * {@code effectiveFrom}。边界之前已经定稿的窗口维持原判，之后定稿的窗口按新版本判定。
 *
 * <p>原子性：一条事件对“标签聚合 / trace 聚合 / trace 拓扑”的计入要么全部成功、要么
 * 全部不发生。首次结论（含确定性拒绝）幂等登记，任何重试都得到同一结论。
 */
public class IngestionEngine {

    /** 队列任务：事件或规则变更二选一。 */
    private record Task(TelemetryEvent event, RuleChangeRequest change,
                       CompletableFuture<IngestResult> eventFuture,
                       CompletableFuture<RuleChangeResult> changeFuture) {
        static Task event(TelemetryEvent e, CompletableFuture<IngestResult> f) {
            return new Task(e, null, f, null);
        }

        static Task change(RuleChangeRequest c, CompletableFuture<RuleChangeResult> f) {
            return new Task(null, c, null, f);
        }

        boolean isChange() {
            return change != null;
        }
    }

    private final TelemetryProperties props;
    private final WatermarkGenerator watermark;
    private final WindowStore windowStore;
    private final TraceRegistry traceRegistry;
    private final AlertEngine alertEngine;
    private final EventDeduper deduper;
    private final IngestionMetrics metrics;
    private final EventJournal journal;
    private final RuleVersionStore ruleStore;
    private final RuleManager ruleManager;

    private final LinkedBlockingQueue<Task> queue;
    private final AtomicInteger queueDepth = new AtomicInteger();
    private final ExecutorService worker;
    private final ScheduledExecutorService scheduler;
    private final int maxTraceSeriesPerWindow;
    private volatile boolean running = true;

    /** 兼容旧装配：无版本化规则运营、无复盘日志。 */
    public IngestionEngine(TelemetryProperties props,
                           WatermarkGenerator watermark, WindowStore windowStore,
                           TraceRegistry traceRegistry, AlertEngine alertEngine,
                           EventDeduper deduper, IngestionMetrics metrics) {
        this(props, watermark, windowStore, traceRegistry, alertEngine, deduper, metrics,
                null, null, new RuleManager());
    }

    public IngestionEngine(TelemetryProperties props,
                           WatermarkGenerator watermark, WindowStore windowStore,
                           TraceRegistry traceRegistry, AlertEngine alertEngine,
                           EventDeduper deduper, IngestionMetrics metrics,
                           EventJournal journal, RuleVersionStore ruleStore,
                           RuleManager ruleManager) {
        this.props = props;
        this.watermark = watermark;
        this.windowStore = windowStore;
        this.traceRegistry = traceRegistry;
        this.alertEngine = alertEngine;
        this.deduper = deduper;
        this.metrics = metrics;
        this.journal = journal;
        this.ruleStore = ruleStore;
        this.ruleManager = ruleManager;
        this.queue = new LinkedBlockingQueue<>(props.getBuffer().getQueueCapacity());
        this.maxTraceSeriesPerWindow = props.getCardinality().getMaxTraceSeriesPerWindow();

        this.worker = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "telemetry-ingest-worker");
            t.setDaemon(true);
            return t;
        });
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "telemetry-watermark-tick");
            t.setDaemon(true);
            return t;
        });
        this.worker.submit(this::workLoop);
        long tickMs = Math.max(1L, props.getWindow().getTickInterval().toMillis());
        this.scheduler.scheduleAtFixedRate(this::onTick, tickMs, tickMs, TimeUnit.MILLISECONDS);
    }

    public IngestResult ingest(TelemetryEvent event) {
        // 1) 字段校验（确定拒绝）
        IngestResult invalid = validate(event);
        if (invalid != null) {
            metrics.incrementRejected(RejectReason.INVALID_EVENT);
            return invalid;
        }
        // 2) 有界队列背压：超时快速失败（瞬态拒绝，不幂等登记，允许重试）。
        Task task = Task.event(event, new CompletableFuture<>());
        if (!offer(task)) {
            metrics.incrementRejected(RejectReason.BUFFER_FULL);
            return IngestResult.rejected(event.eventId(), RejectReason.BUFFER_FULL,
                    "摄取缓冲已满，触发背压拒绝");
        }
        metrics.observeQueueDepth(queueDepth.incrementAndGet());
        try {
            return task.eventFuture().get(props.getBuffer().getResultTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            metrics.incrementRejected(RejectReason.PROCESSING_TIMEOUT);
            return IngestResult.rejected(event.eventId(), RejectReason.PROCESSING_TIMEOUT,
                    "等待处理结果超时；重试将依据幂等表得到确定结论");
        }
    }

    /**
     * 在线变更规则（不重启、不断流）。请求与事件进入同一队列、同一 worker 线性化；
     * 相同 requestId 的重试返回首次结论。容量/超时以可区分状态快速失败，不挂起调用方。
     */
    public RuleChangeResult changeRule(RuleChangeRequest request) {
        String invalid = ruleManager.validate(request);
        if (invalid != null) {
            return RuleChangeResult.invalid(request, invalid);
        }
        Task task = Task.change(request, new CompletableFuture<>());
        if (!offer(task)) {
            return new RuleChangeResult(request.requestId(), request.id(),
                    RuleChangeStatus.BUFFER_FULL, null, null,
                    "规则运营队列已满（与摄取共享背压），请稍后重试");
        }
        metrics.observeQueueDepth(queueDepth.incrementAndGet());
        try {
            return task.changeFuture().get(props.getBuffer().getResultTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return new RuleChangeResult(request.requestId(), request.id(),
                    RuleChangeStatus.PROCESSING_TIMEOUT, null, null,
                    "等待规则变更结果超时；可携带相同 requestId 重试（幂等）");
        }
    }

    private boolean offer(Task task) {
        try {
            return queue.offer(task, props.getBuffer().getOfferTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void workLoop() {
        while (running || !queue.isEmpty()) {
            try {
                Task task = queue.poll(500, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                queueDepth.decrementAndGet();
                if (task.isChange()) {
                    task.changeFuture().complete(processRuleChange(task.change()));
                } else {
                    task.eventFuture().complete(process(task.event()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // 单条任务的任何意外都不能杀死 worker。
            }
        }
    }

    // ------------------------------------------------------------------
    // 规则变更（worker 单线程内执行，与事件严格线性化）
    // ------------------------------------------------------------------
    private RuleChangeResult processRuleChange(RuleChangeRequest request) {
        RuleChangeResult first = ruleManager.firstResult(request.requestId());
        if (first != null) {
            return first;
        }
        long seq = journal == null ? -1L : journal.nextSeq();
        long effectiveFrom = watermark.currentWatermark();
        if (effectiveFrom == WatermarkGenerator.none()) {
            // 尚无事件时水位线不存在：首个窗口之前都算旧规则，给最小边界。
            effectiveFrom = Long.MIN_VALUE;
        }
        RuleVersion latest = ruleStore.latest(request.id());
        RuleManager.MergedSpec spec = ruleManager.merge(request, latest);
        String note = RuleManager.describeChange(request, latest, spec);
        RuleVersion version = ruleStore.append(request.id(), spec.enabled(), spec.scope(),
                spec.service(), spec.metric(), spec.tagSelector(), spec.threshold(),
                spec.operator(), spec.quietFromMillis(), spec.quietUntilMillis(),
                effectiveFrom, System.currentTimeMillis(), note);
        if (journal != null) {
            journal.appendRuleChange(seq, version,
                    watermark.currentWatermark() == WatermarkGenerator.none()
                            ? Long.MIN_VALUE : watermark.currentWatermark());
        }
        RuleChangeResult result = new RuleChangeResult(request.requestId(), request.id(),
                RuleChangeStatus.APPLIED, version.version(), effectiveFrom, note);
        ruleManager.remember(request.requestId(), result);
        return result;
    }

    // ------------------------------------------------------------------
    // 事件计入（worker 单线程内执行）
    // ------------------------------------------------------------------
    private IngestResult process(TelemetryEvent event) {
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

        IngestResult tagReject = checkTagShape(event);
        if (tagReject != null) {
            metrics.incrementRejected(RejectReason.TAG_CARDINALITY_LIMIT);
            return tagReject;
        }

        if (deduper.size() >= props.getCardinality().getMaxEventIds()) {
            return rejectAndCommit(event, RejectReason.TOO_MANY_EVENT_IDS,
                    "事件 ID 去重池已达上限 "
                            + props.getCardinality().getMaxEventIds());
        }

        java.util.Optional<RejectReason> r1 = windowStore.add(event, windowStart);
        if (r1.isPresent()) {
            return rejectAndCommit(event, r1.get(),
                    "窗口 " + windowStart + " 内标签序列数已达上限 "
                            + props.getCardinality().getMaxSeriesPerWindow());
        }
        java.util.Optional<RejectReason> r2 =
                windowStore.addTrace(event, windowStart, maxTraceSeriesPerWindow);
        if (r2.isPresent()) {
            windowStore.removeLast(event, windowStart);
            return rejectAndCommit(event, r2.get(),
                    "窗口 " + windowStart + " 内 trace 序列数已达上限 "
                            + maxTraceSeriesPerWindow);
        }
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

        IngestResult accepted = IngestResult.accepted(event.eventId(), windowStart);
        boolean committed = deduper.commit(event, accepted);
        if (!committed) {
            windowStore.removeLast(event, windowStart);
            windowStore.removeLastTrace(event, windowStart);
            return rejectAndCommit(event, RejectReason.TOO_MANY_EVENT_IDS,
                    "事件 ID 去重池已满，提交幂等登记失败");
        }
        metrics.incrementAccepted();

        // 序列号与日志追加都在 seal 之前：事件序列号严格小于它触发的告警事实序列号，
        // 复盘按 seq 线性重放时顺序与实时完全一致。
        long eventSeq = journal == null ? -1L : journal.nextSeq();
        long droppedEventsBefore = journal == null ? 0L : journal.droppedEvents();
        watermark.observe(event.eventTimeMillis());
        if (journal != null) {
            journal.appendEvent(eventSeq, event, windowStart, watermark.currentWatermark());
        }
        sealWindowsAndEvaluate();
        if (journal != null) {
            // 环形日志淘汰最老事件时，在推进后的下界处固化告警检查点（水位线单调，覆盖即可），
            // 使保留下界之后的复盘仍能延续 breach/recovery 计数与状态。
            if (journal.droppedEvents() > droppedEventsBefore) {
                journal.saveCheckpoint(alertEngine.checkpoint(watermark.currentWatermark()));
            }
        }
        return accepted;
    }

    private void sealWindowsAndEvaluate() {
        long wm = watermark.currentWatermark();
        List<WindowAggregate> sealed =
                windowStore.sealUpTo(wm, props.getWindow().getSizeMillis());
        for (WindowAggregate aggregate : sealed) {
            alertEngine.onWindowSealed(aggregate);
            for (WindowStore.TraceAggregate ta :
                    windowStore.sealedTraceAggregates(aggregate.windowStartMillis())) {
                TraceSnapshot snapshot = traceRegistry.snapshot(ta.traceId());
                alertEngine.onTraceWindowSealed(
                        ta.traceId(), ta.service(), ta.metric(), ta.avg(), ta.count(),
                        ta.windowStartMillis(), ta.windowEndMillis(),
                        snapshot == null ? null : snapshot.status(),
                        snapshot == null ? List.of() : snapshot.reasons());
            }
        }
        long size = props.getWindow().getSizeMillis();
        long lateness = props.getWindow().getAllowedLatenessMillis();
        deduper.prune(wm - size - lateness);
    }

    private void onTick() {
        try {
            long before = watermark.currentWatermark();
            long after = watermark.tick();
            if (after != before) {
                sealWindowsAndEvaluate();
            }
            traceRegistry.pruneIdle(System.currentTimeMillis(),
                    props.getTrace().getRetention().toMillis());
        } catch (RuntimeException e) {
            // tick 异常不能杀死调度线程
        }
    }

    private IngestResult rejectAndCommit(TelemetryEvent event, RejectReason reason, String detail) {
        metrics.incrementRejected(reason);
        return commitReject(event, IngestResult.rejected(event.eventId(), reason, detail));
    }

    private IngestResult commitReject(TelemetryEvent event, IngestResult result) {
        deduper.commit(event, result);
        return result;
    }

    private IngestResult validate(TelemetryEvent event) {
        if (event == null) {
            return IngestResult.rejected("<null>", RejectReason.INVALID_EVENT, "请求体为空");
        }
        if (event.eventId() == null || event.eventId().isBlank()) {
            return IngestResult.rejected("<missing>", RejectReason.INVALID_EVENT, "eventId 缺失");
        }
        if (event.eventTimeMillis() < 0) {
            return IngestResult.rejected(event.eventId(), RejectReason.INVALID_EVENT,
                    "eventTimeMillis 不能为负: " + event.eventTimeMillis());
        }
        return null;
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

    public int queueDepth() {
        return queueDepth.get();
    }

    public IngestionMetrics metrics() {
        return metrics;
    }

    public EventJournal journal() {
        return journal;
    }

    public RuleVersionStore ruleStore() {
        return ruleStore;
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        scheduler.shutdownNow();
        worker.shutdown();
    }
}

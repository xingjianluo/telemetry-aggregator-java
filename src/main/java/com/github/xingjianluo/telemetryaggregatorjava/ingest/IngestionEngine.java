package com.github.xingjianluo.telemetryaggregatorjava.ingest;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.dedup.EventDeduper;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
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
 * 摄取编排。
 *
 * <p>并发模型：调用方线程只做字段校验，然后进入有界队列；<b>单一 worker 线程</b>
 * 顺序完成水位线推进、窗口计数、调用链登记、窗口关闭与告警评估，从结构上消除计数丢失与
 * 状态错乱。队列满时按 {@code offerTimeout} 快速失败（BUFFER_FULL），绝不长时间阻塞。
 *
 * <p>原子性：一条事件对“标签聚合 / trace 聚合 / trace 拓扑”的计入要么全部成功、要么全部
 * 不发生（对前两步做补偿回滚）。首次结论（含确定性拒绝）会幂等登记，任何重试都得到同一
 * 结论，重复投递不可能造成二次计数。
 */
public class IngestionEngine {

    private record Task(TelemetryEvent event, CompletableFuture<IngestResult> future) {
    }

    private final TelemetryProperties props;
    private final WatermarkGenerator watermark;
    private final WindowStore windowStore;
    private final TraceRegistry traceRegistry;
    private final AlertEngine alertEngine;
    private final EventDeduper deduper;
    private final IngestionMetrics metrics;

    private final LinkedBlockingQueue<Task> queue;
    private final AtomicInteger queueDepth = new AtomicInteger();
    private final ExecutorService worker;
    private final ScheduledExecutorService scheduler;
    private final int maxTraceSeriesPerWindow;
    private volatile boolean running = true;

    public IngestionEngine(TelemetryProperties props,
                           WatermarkGenerator watermark, WindowStore windowStore,
                           TraceRegistry traceRegistry, AlertEngine alertEngine,
                           EventDeduper deduper, IngestionMetrics metrics) {
        this.props = props;
        this.watermark = watermark;
        this.windowStore = windowStore;
        this.traceRegistry = traceRegistry;
        this.alertEngine = alertEngine;
        this.deduper = deduper;
        this.metrics = metrics;
        this.queue = new LinkedBlockingQueue<>(props.getBuffer().getQueueCapacity());
        // trace 序列基数独立保护，不占用标签序列预算。
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
        //    幂等检查刻意放在 worker 单线程内与 commit 线性化，杜绝并发重放双计数。
        Task task = new Task(event, new CompletableFuture<>());
        boolean offered;
        try {
            offered = queue.offer(task, props.getBuffer().getOfferTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return IngestResult.rejected(event.eventId(), RejectReason.BUFFER_FULL,
                    "入队被中断");
        }
        if (!offered) {
            metrics.incrementRejected(RejectReason.BUFFER_FULL);
            return IngestResult.rejected(event.eventId(), RejectReason.BUFFER_FULL,
                    "摄取缓冲已满，触发背压拒绝");
        }
        int depth = queueDepth.incrementAndGet();
        metrics.observeQueueDepth(depth);

        try {
            return task.future().get(props.getBuffer().getResultTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            metrics.incrementRejected(RejectReason.PROCESSING_TIMEOUT);
            return IngestResult.rejected(event.eventId(), RejectReason.PROCESSING_TIMEOUT,
                    "等待处理结果超时；重试将依据幂等表得到确定结论");
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
                IngestResult result = process(task.event());
                task.future().complete(result);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // 单条事件的任何意外都不能杀死 worker；以显式拒绝收尾。
            }
        }
    }

    /** worker 单线程内执行：全部状态变更在此线性化。 */
    private IngestResult process(TelemetryEvent event) {
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

        // 3.1 迟到 / 窗口已关闭（确定拒绝，幂等登记，重试永远得到同一结论）
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

        // 3.2 标签形状基数检查（键值数量/长度）。
        //     格式错误不登记幂等表、不占用 ID 池：同一 ID 修正载荷后应能被接受。
        IngestResult tagReject = checkTagShape(event);
        if (tagReject != null) {
            metrics.incrementRejected(RejectReason.TAG_CARDINALITY_LIMIT);
            return tagReject;
        }

        // 3.3 去重表基数：必须在产生任何计数之前预留登记名额
        //      （commit 在全部成功后执行；此处先探测容量，容量不足即确定拒绝）
        if (deduper.size() >= props.getCardinality().getMaxEventIds()) {
            return rejectAndCommit(event, RejectReason.TOO_MANY_EVENT_IDS,
                    "事件 ID 去重池已达上限 "
                            + props.getCardinality().getMaxEventIds());
        }

        // 3.4 计入窗口标签聚合
        java.util.Optional<RejectReason> r1 = windowStore.add(event, windowStart);
        if (r1.isPresent()) {
            return rejectAndCommit(event, r1.get(),
                    "窗口 " + windowStart + " 内标签序列数已达上限 "
                            + props.getCardinality().getMaxSeriesPerWindow());
        }
        // 3.5 计入 trace × 窗口聚合；失败则补偿回滚标签计数（实现见 WindowStore.removeLast）
        java.util.Optional<RejectReason> r2 =
                windowStore.addTrace(event, windowStart, maxTraceSeriesPerWindow);
        if (r2.isPresent()) {
            windowStore.removeLast(event, windowStart);
            return rejectAndCommit(event, r2.get(),
                    "窗口 " + windowStart + " 内 trace 序列数已达上限 "
                            + maxTraceSeriesPerWindow);
        }
        // 3.6 调用链拓扑登记；失败则补偿前两步
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

        // 4) 全部计入成功：登记幂等首次结论、推进水位线、关闭到期窗口、驱动告警
        IngestResult accepted = IngestResult.accepted(event.eventId(), windowStart);
        boolean committed = deduper.commit(event, accepted);
        if (!committed) {
            // 极端竞争（理论上仅 worker 单线程写）：回滚全部计数
            windowStore.removeLast(event, windowStart);
            windowStore.removeLastTrace(event, windowStart);
            return rejectAndCommit(event, RejectReason.TOO_MANY_EVENT_IDS,
                    "事件 ID 去重池已满，提交幂等登记失败");
        }
        metrics.incrementAccepted();

        watermark.observe(event.eventTimeMillis());
        sealWindowsAndEvaluate();
        return accepted;
    }

    private void sealWindowsAndEvaluate() {
        long wm = watermark.currentWatermark();
        List<WindowAggregate> sealed =
                windowStore.sealUpTo(wm, props.getWindow().getSizeMillis());
        for (WindowAggregate aggregate : sealed) {
            alertEngine.onWindowSealed(aggregate);
            // trace 作用域告警：用同一批定稿窗口数据，按 trace 评估并附带拓扑标记
            for (WindowStore.TraceAggregate ta :
                    windowStore.sealedTraceAggregates(aggregate.windowStartMillis())) {
                TraceSnapshot snapshot = traceRegistry.snapshot(ta.traceId());
                alertEngine.onTraceWindowSealed(
                        ta.traceId(), ta.service(), ta.metric(), ta.avg(), ta.count(),
                        ta.windowStartMillis(),
                        snapshot == null ? null : snapshot.status(),
                        snapshot == null ? List.of() : snapshot.reasons(),
                        System.currentTimeMillis());
            }
        }
        // 老化去重表：只有事件时间 < wm - size - lateness 的条目才可回收——
        // 其窗口必然已关闭超过一个窗口龄期，任何重放都会被确定性地判为迟到；
        // 其余（含刚关闭但仍在迟到容忍内窗口的）条目一律保留，杜绝重放双计数。
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
        // 确定性拒绝也登记首次结论：后续相同 ID 重试直接复用，保证可复现、不计数
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

    @PreDestroy
    public void shutdown() {
        running = false;
        scheduler.shutdownNow();
        worker.shutdown();
    }
}

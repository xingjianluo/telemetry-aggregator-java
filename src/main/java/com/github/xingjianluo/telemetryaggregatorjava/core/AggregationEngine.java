package com.github.xingjianluo.telemetryaggregatorjava.core;

import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.model.ChainView;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowResult;
import com.github.xingjianluo.telemetryaggregatorjava.stats.EngineStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 遥测聚合引擎：摄取 / 聚合 / 告警 / 查询共用同一串行执行上下文，语义严格对齐。
 *
 * <p>并发模型：</p>
 * <ul>
 *   <li>{@link #ingest} 在 HTTP 线程上做基本校验与幂等去重，然后把事件
 *       <b>非阻塞</b>放入容量为 {@code ingestQueueCapacity} 的有界队列；
 *       队列满立即以 {@code QUEUE_FULL} 拒绝并回滚去重新占位（写入永不长时间阻塞）。</li>
 *   <li>单个工作线程按 FIFO 串行处理：水位线推进 → 窗口基数校验与计数 →
 *       窗口关闭 → 调用链还原 → 告警评估 → 链路剪枝。结论确定且可复现。</li>
 *   <li>查询/水位线推进走优先控制队列：工作线程先排空当前事件再执行控制任务，
 *       因此调用方等待到的是“此前所有已入队事件处理完”的一致快照。</li>
 *   <li>同一 eventId 由条纹锁互斥，保证“首次投递入队失败回滚”与“重试判重”
 *       不会竞争；重复投递共享首次的 future，结论逐字节一致。</li>
 * </ul>
 */
public final class AggregationEngine {

    private static final Logger log = Logger.getLogger(AggregationEngine.class.getName());
    private static final int STRIPE_COUNT = 64;

    /** 工作队列任务。 */
    private interface Task {
        void run();
    }

    private final TelemetryProperties props;
    private final EngineStats stats;

    private final WatermarkManager watermarks;
    private final DedupTable dedup;
    private final WindowTable windows;
    private final ChainTracker chains;
    private final AlertEngine alerts;

    private final ArrayBlockingQueue<Task> eventQueue;
    private final ArrayBlockingQueue<Task> controlQueue = new ArrayBlockingQueue<>(4096);
    private final ReentrantLock[] stripes;

    private volatile boolean started;
    private volatile boolean shuttingDown;
    private Thread worker;

    public AggregationEngine(TelemetryProperties props) {
        this.props = props;
        this.stats = new EngineStats();
        this.watermarks = new WatermarkManager(props.getAllowedLatenessMillis());
        this.dedup = new DedupTable(props.getDedupCapacity());
        this.windows = new WindowTable(props.getWindowMillis(),
                props.getMaxTagValuesPerKey(), props.getMaxSeriesPerWindow(),
                props.getMaxActiveWindows(), props.getRetainedWindows());
        this.chains = new ChainTracker(props.getMaxSpans());
        this.alerts = new AlertEngine(props.getRetainedAlertEvents());
        this.eventQueue = new ArrayBlockingQueue<>(props.getIngestQueueCapacity());
        this.stripes = new ReentrantLock[STRIPE_COUNT];
        for (int i = 0; i < STRIPE_COUNT; i++) {
            stripes[i] = new ReentrantLock();
        }
    }

    public void start() {
        if (started) {
            return;
        }
        started = true;
        worker = new Thread(this::runLoop, "telemetry-aggregator-worker");
        worker.setDaemon(true);
        worker.start();
        log.info(() -> "AggregationEngine started: windowMillis=" + props.getWindowMillis()
                + ", allowedLatenessMillis=" + props.getAllowedLatenessMillis()
                + ", queueCapacity=" + props.getIngestQueueCapacity());
    }

    public void stop() {
        if (!started) {
            return;
        }
        shuttingDown = true;
        try {
            worker.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        started = false;
    }

    // ------------------------------------------------------------------
    // 摄取
    // ------------------------------------------------------------------

    /**
     * 非阻塞摄取。
     *
     * <p>处理顺序（HTTP 线程）：校验 → 条纹锁内判重 → 入有界队列。
     * 首次投递返回其独占 future，工作线程处理完后兑现；重复投递直接返回首次的
     * future，因此任何重试都得到一致结论，且绝不重复计数。</p>
     */
    public CompletableFuture<IngestResult> ingest(TelemetryEvent event) {
        stats.recordReceived();

        String validationError = validate(event);
        if (validationError != null) {
            return reject(IngestStatus.INVALID, validationError, event, null);
        }
        if (shuttingDown) {
            return reject(IngestStatus.SHUTDOWN, "engine is shutting down", event, null);
        }

        String eventId = event.getEventId();
        String fingerprint = DedupTable.fingerprint(event);
        ReentrantLock lock = stripes[stripeIndex(eventId)];
        lock.lock();
        try {
            CompletableFuture<IngestResult> firstFuture = new CompletableFuture<>();
            DedupTable.Classification c = dedup.classify(eventId, fingerprint, firstFuture);
            switch (c.getDecision()) {
                case CAPACITY -> {
                    return reject(IngestStatus.DEDUP_CAPACITY,
                            "dedup table capacity " + props.getDedupCapacity()
                                    + " reached for new eventId", event, null);
                }
                case CONFLICT -> {
                    return reject(IngestStatus.ID_CONFLICT,
                            "eventId '" + eventId
                                    + "' already exists with a different payload",
                            event, null);
                }
                case DUPLICATE -> {
                    stats.recordDedupReplay();
                    log.fine(() -> "dedup replay for eventId '" + eventId
                            + "' -> reuse first conclusion (no double count)");
                    return c.getEntry().future();
                }
                case NEW -> {
                    // 迟到判定只在工作线程按 FIFO 处理时做一次（权威）：
                    // 入口不判晚，避免事件与“推进水位线”并发时因抢先看到高水位线
                    // 而把最终会被接受的事件多余拒绝。
                    Task task = () -> processNewEvent(event, c.getEntry());
                    if (!eventQueue.offer(task)) {
                        // 队列满：回滚 NEW 占位并立即拒绝；写入线程不被阻塞
                        dedup.rollbackNew(eventId);
                        stats.setQueueDepth(eventQueue.size());
                        return reject(IngestStatus.QUEUE_FULL,
                                "bounded ingest queue is full (capacity "
                                        + props.getIngestQueueCapacity()
                                        + "); backpressure rejection, event not counted",
                                event, null);
                    }
                    stats.setQueueDepth(eventQueue.size());
                    return firstFuture;
                }
                default -> throw new IllegalStateException("unknown decision");
            }
        } finally {
            lock.unlock();
        }
    }

    private static String validate(TelemetryEvent e) {
        if (e == null) {
            return "event body is null";
        }
        if (e.getEventId() == null || e.getEventId().isBlank()) {
            return "eventId is required (idempotency key)";
        }
        if (e.getMetric() == null || e.getMetric().isBlank()) {
            return "metric is required";
        }
        if (e.getTimestampMillis() <= 0) {
            return "timestampMillis must be a positive event-time (epoch millis)";
        }
        if (e.getTags() != null) {
            for (Map.Entry<String, String> t : e.getTags().entrySet()) {
                if (t.getKey() == null || t.getKey().isEmpty() || t.getValue() == null) {
                    return "tag keys must be non-empty and tag values non-null";
                }
            }
        }
        if (e.getTraceId() != null && !e.getTraceId().isBlank()
                && (e.getSpanId() == null || e.getSpanId().isBlank())) {
            return "spanId is required when traceId is present";
        }
        return null;
    }

    private CompletableFuture<IngestResult> reject(IngestStatus status, String reason,
                                                   TelemetryEvent event,
                                                   Long windowStart) {
        stats.recordStatus(status);
        long ts = event == null ? 0L : event.getTimestampMillis();
        long wm = watermarks.currentWatermark();
        IngestResult result = new IngestResult(status, reason, ts, windowStart,
                wm == Long.MIN_VALUE ? null : wm);
        log.fine(() -> "ingest rejected: " + brief(event) + " -> " + status + " : " + reason);
        return CompletableFuture.completedFuture(result);
    }

    private static String brief(TelemetryEvent e) {
        if (e == null) {
            return "<null>";
        }
        return "eventId=" + e.getEventId() + ", eventTime=" + e.getTimestampMillis()
                + ", metric=" + e.getMetric();
    }

    private static int stripeIndex(String eventId) {
        return Math.floorMod(eventId.hashCode(), STRIPE_COUNT);
    }

    // ------------------------------------------------------------------
    // 工作线程
    // ------------------------------------------------------------------

    private void runLoop() {
        final List<Task> drained = new ArrayList<>();
        while (true) {
            // 先排空当前所有已排队的摄取事件（FIFO），保证后续控制任务看到一致状态
            drainEvents(drained);

            if (!shuttingDown && eventQueue.isEmpty()) {
                Task control;
                try {
                    control = controlQueue.poll(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (control != null) {
                    // 关键：控制任务可能在 poll 等待期间入队，而此刻仍有摄取事件在
                    // poll 前后进入 eventQueue。执行控制任务前必须再排空一次事件，
                    // 否则“推进水位线”会抢先于这些事件执行，把它们误判为迟到。
                    drainEvents(drained);
                    runTask(control);
                    publishGauges();
                }
            }

            if (shuttingDown && eventQueue.isEmpty() && controlQueue.isEmpty()) {
                break;
            }
        }
        // 退出循环前两队列均已空；这里兜底排空任何残余任务（幂等）
        do {
            drainEvents(drained);
        } while (!eventQueue.isEmpty());
        controlQueue.drainTo(drained);
        drained.forEach(this::runTask);
    }

    /** 排空并按顺序执行当前 eventQueue 中的全部任务。 */
    private void drainEvents(List<Task> drained) {
        eventQueue.drainTo(drained);
        for (Task t : drained) {
            runTask(t);
        }
        drained.clear();
        publishGauges();
    }

    private void runTask(Task t) {
        try {
            t.run();
        } catch (RuntimeException e) {
            log.log(Level.SEVERE, "task failed in worker", e);
        }
    }

    private void processNewEvent(TelemetryEvent event, DedupTable.Entry entry) {
        long wmBefore = watermarks.currentWatermark();

        IngestResult result;
        // 工作线程按 FIFO 处理：若该事件入队后、轮到它之前水位线已越过其事件时间
        // （FIFO 内更晚提交的更大时间戳先被处理），则其窗口已关闭，判迟到拒绝。
        if (watermarks.isLate(event.getTimestampMillis())) {
            result = finish(entry, new IngestResult(IngestStatus.LATE_BEHIND_WATERMARK,
                    "eventTime " + event.getTimestampMillis() + " < watermark "
                            + wmBefore
                            + " when processed in FIFO order; window already"
                            + " finalized, rejected (not counted)",
                    event.getTimestampMillis(),
                    windows.windowStartOf(event.getTimestampMillis()), wmBefore));
            log.info(() -> "late event rejected: " + brief(event)
                    + " watermark=" + wmBefore);
            return;
        }

        // 先尝试调用链记录：冲突类事件不可计数
        ChainTracker.AddOutcome chainOutcome = null;
        if (event.getTraceId() != null && !event.getTraceId().isBlank()) {
            chainOutcome = chains.addSpan(event);
            if (chainOutcome.isConflict()) {
                result = finish(entry, new IngestResult(IngestStatus.ID_CONFLICT,
                        chainOutcome.getReason(), event.getTimestampMillis(), null,
                        wmBefore));
                log.info("span conflict: " + brief(event) + " : "
                        + chainOutcome.getReason());
                return;
            }
            if (!chainOutcome.isAccepted()) {
                // span 基数上限：整条事件拒绝（窗口也不计数），保证结论可解释
                result = finish(entry, new IngestResult(IngestStatus.TAG_CARDINALITY,
                        chainOutcome.getReason(), event.getTimestampMillis(), null,
                        wmBefore));
                return;
            }
        }

        // 计入窗口（内含标签基数 / 活动窗口数保护）
        WindowTable.ApplyOutcome ao = windows.applyEvent(event, wmBefore);
        if (ao.getStatus() != IngestStatus.ACCEPTED) {
            // 窗口拒绝：回滚已记录的 span（事件未被计数，链路同样不应保留它）
            if (chainOutcome != null) {
                chains.rollbackLastSpan(event.getTraceId(), event.getSpanId());
            }
            result = finish(entry, new IngestResult(ao.getStatus(), ao.getReason(),
                    event.getTimestampMillis(), ao.getWindowStart(), wmBefore));
            log.info(() -> "event rejected by window guard: " + brief(event)
                    + " -> " + ao.getStatus() + " : " + ao.getReason());
            return;
        }

        // 推进水位线并关闭到点窗口
        long wmAfter = watermarks.observe(event.getTimestampMillis());
        stats.observeEventTime(event.getTimestampMillis());
        List<WindowResult> closedNow = windows.closeUpTo(wmAfter);
        if (!closedNow.isEmpty()) {
            closedNow.forEach(w -> {
                stats.incrementClosedWindows();
                log.fine(() -> "window closed [" + w.getWindowStart() + ","
                        + w.getWindowEnd() + ") at watermark " + w.getClosedAtWatermark()
                        + " with " + w.getSeries().size() + " series");
            });
            // 窗口关闭后剪枝过窗链路并评估告警
            chains.prune(wmAfter, props.getWindowMillis());
        }

        // 告警评估（链路事件才进行）
        if (chainOutcome != null) {
            ChainView view = chainOutcome.getView();
            List<AlertEvent> alertEvents = alerts.evaluate(
                    event.getTraceId(), view, event.getTimestampMillis());
            alertEvents.forEach(a -> log.info(() -> "ALERT " + a.getState()
                    + " rule=" + a.getRuleId() + " trace=" + a.getTraceId()
                    + " value=" + a.getObservedValue() + " : " + a.getReason()));
        }

        result = finish(entry, new IngestResult(IngestStatus.ACCEPTED,
                ao.getReason(), event.getTimestampMillis(), ao.getWindowStart(),
                wmAfter == Long.MIN_VALUE ? null : wmAfter));
        log.fine(() -> "accepted: " + brief(event) + " windowStart="
                + ao.getWindowStart() + " watermark=" + wmAfter);
    }

    private IngestResult finish(DedupTable.Entry entry, IngestResult result) {
        stats.recordStatus(result.getStatus());
        entry.future().complete(result);
        entry.markCompleted();
        return result;
    }

    private void publishGauges() {
        stats.setQueueDepth(eventQueue.size());
        stats.setDedupSize(dedup.size());
        stats.setSpanCount(chains.spanCount());
        stats.setActiveWindows(windows.activeWindowCount());
        stats.updateWatermark(watermarks.currentWatermark());
    }

    // ------------------------------------------------------------------
    // 控制面（全部在工作线程串行执行）
    // ------------------------------------------------------------------

    public void awaitDrained(long timeoutMillis) {
        if (!started) {
            return;
        }
        submitControl(() -> {
        }, timeoutMillis);
    }

    public void advanceWatermarkTo(long eventTimeMillis) {
        if (!started) {
            doAdvance(eventTimeMillis);
            return;
        }
        submitControl(() -> doAdvance(eventTimeMillis), 10_000);
    }

    private void doAdvance(long eventTimeMillis) {
        long wm = watermarks.advanceTo(eventTimeMillis);
        stats.observeEventTime(eventTimeMillis);
        List<WindowResult> closedNow = windows.closeUpTo(wm);
        closedNow.forEach(w -> {
            stats.incrementClosedWindows();
            log.info(() -> "watermark advance closed window [" + w.getWindowStart()
                    + "," + w.getWindowEnd() + ") at " + wm);
        });
        chains.prune(wm, props.getWindowMillis());
    }

    public List<WindowResult> queryWindows(Long from, Long to) {
        if (!started) {
            return windows.closedWindows(from, to);
        }
        return submitControl(() -> windows.closedWindows(from, to), 10_000);
    }

    public ChainView queryChain(String traceId) {
        if (!started) {
            return chains.getChain(traceId);
        }
        return submitControl(() -> chains.getChain(traceId), 10_000);
    }

    public List<ChainView> queryChains() {
        if (!started) {
            return chains.allChains();
        }
        return submitControl(chains::allChains, 10_000);
    }

    public void addAlertRule(AlertRule rule) {
        if (!started) {
            alerts.addRule(rule);
            return;
        }
        submitControl(() -> alerts.addRule(rule), 10_000);
    }

    public boolean removeAlertRule(String ruleId) {
        if (!started) {
            return alerts.removeRule(ruleId);
        }
        return Boolean.TRUE.equals(submitControl(() -> alerts.removeRule(ruleId), 10_000));
    }

    public List<AlertEvent> queryAlertEvents(int limit) {
        if (!started) {
            return alerts.recentEvents(limit);
        }
        return submitControl(() -> alerts.recentEvents(limit), 10_000);
    }

    public Map<String, AlertEngine.RuleStateView> alertStates() {
        if (!started) {
            return alerts.states();
        }
        return submitControl(alerts::states, 10_000);
    }

    private <T> T submitControl(java.util.function.Supplier<T> action, long timeoutMillis) {
        CompletableFuture<T> done = new CompletableFuture<>();
        enqueueControl(() -> {
            try {
                done.complete(action.get());
            } catch (RuntimeException e) {
                done.completeExceptionally(e);
            }
        });
        try {
            return done.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("control task failed/timed out", e);
        }
    }

    private void submitControl(Runnable action, long timeoutMillis) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        enqueueControl(() -> {
            try {
                action.run();
                done.complete(null);
            } catch (RuntimeException e) {
                done.completeExceptionally(e);
            }
        });
        try {
            done.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("control task failed/timed out", e);
        }
    }

    private void enqueueControl(Task t) {
        if (shuttingDown) {
            throw new IllegalStateException("engine is shutting down");
        }
        try {
            if (!controlQueue.offer(t, 5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("control queue saturated");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted enqueueing control task", e);
        }
    }

    public EngineStats getStats() {
        return stats;
    }

    /**
     * 测试辅助：在工作线程上执行一个任意动作，用于确定性地制造队列堆积。
     * 与查询一样走控制队列，动作执行时此前所有已入队事件均已处理完。
     */
    public void runOnWorker(Runnable action, long timeoutMillis) {
        submitControl(action, timeoutMillis);
    }

    TelemetryProperties getProps() {
        return props;
    }
}

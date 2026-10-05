package com.github.xingjianluo.telemetryaggregatorjava.ingest;

import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import jakarta.annotation.PreDestroy;

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
 * 顺序完成水位线推进、窗口计数、调用链登记、窗口关闭与告警评估（{@link EventProcessor}），
 * 从结构上消除计数丢失与状态错乱。队列满时按 {@code offerTimeout} 快速失败
 * （BUFFER_FULL），绝不长时间阻塞。
 *
 * <p>规则变更与事件写入共用同一队列：变更任务在 worker 线程内与事件处理线性化，
 * 先按旧规则定稿全部可定稿窗口，再登记新版本及其生效边界，
 * 因此不会出现状态丢失、重复计数或新旧规则互相覆盖。
 */
public class IngestionEngine {

    private sealed interface Task {
    }

    private record EventTask(TelemetryEvent event, CompletableFuture<IngestResult> future)
            implements Task {
    }

    private record ChangeTask(RuleChangeRequest request, CompletableFuture<RuleChange> future)
            implements Task {
    }

    /** 规则变更提交失败（背压 / 超时 / 校验），调用方可区分处理。 */
    public static class RuleChangeException extends RuntimeException {
        public RuleChangeException(String message) {
            super(message);
        }

        public RuleChangeException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * 非法变更（载荷缺字段、未知规则、幂等键冲突等）：永久性失败，
     * 未产生任何版本、不影响已有规则状态；修正载荷后才可能成功。
     */
    public static class RuleChangeInvalidException extends RuleChangeException {
        public RuleChangeInvalidException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * 背压拒绝：变更<b>确定没有生效</b>（未进入处理队列），
     * 稍后以相同请求（相同 requestId）重试即可得到确定结论。
     */
    public static class RuleChangeBackpressureException extends RuleChangeException {
        public RuleChangeBackpressureException(String message) {
            super(message);
        }
    }

    /**
     * 等待回执超时：变更<b>可能已生效、也可能未生效</b>，调用方不得假定任何一侧。
     * 以相同 requestId 重试是安全的：若首次已生效，重试返回首次登记的同一版本与
     * 同一生效边界；若未生效，重试正常落一个版本。两种情况都不会产生第二个版本。
     */
    public static class RuleChangeTimeoutException extends RuleChangeException {
        public RuleChangeTimeoutException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final TelemetryProperties props;
    private final EventProcessor processor;
    private final IngestionMetrics metrics;

    private final LinkedBlockingQueue<Task> queue;
    private final AtomicInteger queueDepth = new AtomicInteger();
    private final ExecutorService worker;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running = true;

    public IngestionEngine(TelemetryProperties props, EventProcessor processor,
                           IngestionMetrics metrics) {
        this.props = props;
        this.processor = processor;
        this.metrics = metrics;
        this.queue = new LinkedBlockingQueue<>(props.getBuffer().getQueueCapacity());

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
        EventTask task = new EventTask(event, new CompletableFuture<>());
        if (!offer(task)) {
            metrics.incrementRejected(RejectReason.BUFFER_FULL);
            return IngestResult.rejected(event.eventId(), RejectReason.BUFFER_FULL,
                    "摄取缓冲已满，触发背压拒绝");
        }
        try {
            return task.future().get(props.getBuffer().getResultTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            metrics.incrementRejected(RejectReason.PROCESSING_TIMEOUT);
            return IngestResult.rejected(event.eventId(), RejectReason.PROCESSING_TIMEOUT,
                    "等待处理结果超时；重试将依据幂等表得到确定结论");
        }
    }

    /**
     * 在线提交规则变更：与事件写入共用同一队列，按到达顺序在 worker 线程内应用。
     * 返回带版本号与生效边界的变更记录。
     *
     * <p>失败语义可区分（均为快速失败，绝不长时间阻塞调用方）：
     * <ul>
     *   <li>{@link RuleChangeInvalidException}：非法变更，未产生版本，重试原样载荷必然再次失败；</li>
     *   <li>{@link RuleChangeBackpressureException}：确定未生效，可安全重试；</li>
     *   <li>{@link RuleChangeTimeoutException}：结果未知，以相同 requestId 重试可收敛到唯一版本。</li>
     * </ul>
     * 请求携带 {@code requestId} 时，任意次重试最多落一个版本，且每次成功返回
     * 同一版本号与同一生效边界。
     */
    public RuleChange submitRuleChange(RuleChangeRequest request) {
        // 入队前快速校验（确定性失败立即返回，不占队列）；权威校验仍在 worker 内重做。
        try {
            processor.validateRuleChange(request);
        } catch (IllegalArgumentException e) {
            throw new RuleChangeInvalidException("非法规则变更（未产生版本）: " + e.getMessage(), e);
        }
        ChangeTask task = new ChangeTask(request, new CompletableFuture<>());
        if (!offer(task)) {
            throw new RuleChangeBackpressureException(
                    "摄取缓冲已满，规则变更未生效（确定未应用），可稍后以相同 requestId 重试");
        }
        try {
            return task.future().get(props.getBuffer().getResultTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new RuleChangeTimeoutException(
                    "等待规则变更回执超时，变更可能已生效也可能未生效；"
                            + "请以相同 requestId 重试，幂等登记保证最多落一个版本"
                            + (request.requestId() == null ? "（本请求未携带 requestId，重试可能产生新版本）" : ""),
                    e);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof IllegalArgumentException) {
                throw new RuleChangeInvalidException(
                        "非法规则变更（未产生版本）: " + cause.getMessage(), cause);
            }
            throw new RuleChangeException(cause.getMessage(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuleChangeTimeoutException(
                    "等待规则变更回执被中断，变更可能已生效也可能未生效；请以相同 requestId 重试", e);
        }
    }

    private boolean offer(Task task) {
        boolean offered;
        try {
            offered = queue.offer(task, props.getBuffer().getOfferTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        if (offered) {
            int depth = queueDepth.incrementAndGet();
            metrics.observeQueueDepth(depth);
        }
        return offered;
    }

    private void workLoop() {
        while (running || !queue.isEmpty()) {
            try {
                Task task = queue.poll(500, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                queueDepth.decrementAndGet();
                if (task instanceof EventTask eventTask) {
                    eventTask.future().complete(processor.process(eventTask.event()));
                } else if (task instanceof ChangeTask changeTask) {
                    try {
                        changeTask.future().complete(processor.applyRuleChange(changeTask.request()));
                    } catch (RuntimeException e) {
                        changeTask.future().completeExceptionally(e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // 单条事件的任何意外都不能杀死 worker；以显式拒绝收尾。
            }
        }
    }

    private void onTick() {
        try {
            processor.onTick();
        } catch (RuntimeException e) {
            // tick 异常不能杀死调度线程
        }
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

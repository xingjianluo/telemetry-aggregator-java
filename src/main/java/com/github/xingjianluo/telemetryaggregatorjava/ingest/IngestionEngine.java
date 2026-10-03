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
     * 返回带版本号与生效边界的变更记录；校验失败 / 背压 / 超时以
     * {@link RuleChangeException} 快速失败，绝不长时间阻塞调用方。
     */
    public RuleChange submitRuleChange(RuleChangeRequest request) {
        ChangeTask task = new ChangeTask(request, new CompletableFuture<>());
        if (!offer(task)) {
            throw new RuleChangeException("摄取缓冲已满，规则变更被背压拒绝，可稍后重试");
        }
        try {
            return task.future().get(props.getBuffer().getResultTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new RuleChangeException(cause.getMessage(), cause);
        } catch (Exception e) {
            throw new RuleChangeException("等待规则变更结果超时或中断: " + e.getMessage(), e);
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

package com.github.xingjianluo.telemetryaggregatorjava.ingest;

import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeOutcome;
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

    private record ChangeTask(RuleChangeRequest request,
                              CompletableFuture<RuleChangeOutcome> future)
            implements Task {
    }

    /** 仅供测试：在 worker 线程内阻塞一段时间，用于确定性制造背压 / 超时。 */
    private record BlockTask(long sleepMillis) implements Task {
    }

    /**
     * 规则变更提交失败的基类（可区分、可据此决定是否重试）。HTTP 层映射为 422/503/408。
     */
    public static class RuleChangeException extends RuntimeException {
        public RuleChangeException(String message) {
            super(message);
        }

        public RuleChangeException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** 真正非法的变更（未知规则、缺定义、id 不一致、幂等键冲突）：快速拒绝、无版本、无副作用。 */
    public static class RuleChangeInvalidException extends RuleChangeException {
        public RuleChangeInvalidException(String message) {
            super(message);
        }
    }

    /**
     * 背压：请求未能进入队列，<b>确定未生效</b>（未登记、无版本）。
     * 用同一 requestId 重试安全，成功后得到确定版本。
     */
    public static class RuleChangeBackpressureException extends RuleChangeException {
        public RuleChangeBackpressureException(String message) {
            super(message);
        }
    }

    /**
     * 等待结果超时：结论<b>不确定</b>——可能根本未生效，也可能已生效只是回执丢失。
     * 必须用同一 requestId 重试：未生效则首次应用并返回版本；已生效则幂等重放
     * （replayed=true）同一版本，绝不会产生第二个版本或第二次状态改动。
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
     *
     * <p>成功返回 {@link RuleChangeOutcome}：首次生效 replayed=false；
     * 同一 requestId 的重试 / 并发重放在已生效后返回 replayed=true，
     * change 永远是同一条记录（版本、生效边界逐位相同），且无任何新副作用。
     *
     * <p>失败以可区分异常快速返回，绝不长时间阻塞调用方：
     * <ul>
     *   <li>{@link RuleChangeInvalidException}：非法变更，确定无版本、无状态改动，无需重试；</li>
     *   <li>{@link RuleChangeBackpressureException}：确定未生效（未入队），用同键重试安全；</li>
     *   <li>{@link RuleChangeTimeoutException}：结论不确定（可能已生效），必须用同键重试。</li>
     * </ul>
     */
    public RuleChangeOutcome submitRuleChange(RuleChangeRequest request) {
        ChangeTask task = new ChangeTask(request, new CompletableFuture<>());
        if (!offer(task)) {
            throw new RuleChangeBackpressureException(
                    "摄取缓冲已满，规则变更被背压拒绝（确定未生效），请用同一 requestId 重试");
        }
        try {
            return task.future().get(props.getBuffer().getResultTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof IllegalArgumentException) {
                // 校验失败发生在 worker 线性化点：快速拒绝，不产生版本、不改动规则状态。
                throw new RuleChangeInvalidException(cause.getMessage());
            }
            throw new RuleChangeException(cause.getMessage(), cause);
        } catch (java.util.concurrent.TimeoutException e) {
            // future 不取消：任务可能正在/将要执行。调用方必须用同一 requestId 重试
            // —— 未生效则得到确定版本，已生效则幂等重放同一版本，绝不产生第二个版本。
            throw new RuleChangeTimeoutException(
                    "等待规则变更结果超时，生效结论不确定，请用同一 requestId 重试以获得确定结论", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuleChangeTimeoutException(
                    "等待规则变更结果时被中断，生效结论不确定，请用同一 requestId 重试", e);
        }
    }

    /**
     * 仅供测试：向 worker 队列头部之外按普通 offer 语义放入一个“占用 worker 若干毫秒”
     * 的阻塞任务。返回 false 表示因背压未能入队（与正常提交同语义）。
     */
    public boolean offerWorkerBlockForTesting(long sleepMillis) {
        return offer(new BlockTask(sleepMillis));
    }

    /** 仅供测试：尝试在不等待结果的情况下入队一个变更任务（用于制造“已入队但未回执”）。 */
    public boolean offerRuleChangeForTesting(RuleChangeRequest request) {
        return offer(new ChangeTask(request, new CompletableFuture<>()));
    }

    private boolean offer(Task task) {        boolean offered;
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
                        RuleChangeRequest req = changeTask.request();
                        // 先查幂等：命中时不再次定稿窗口、不改动状态，直接重放首次记录。
                        RuleChange existing = (req == null || req.requestId() == null
                                || req.requestId().isBlank())
                                ? null
                                : processor.findAppliedChange(req);
                        if (existing != null) {
                            changeTask.future().complete(RuleChangeOutcome.replay(existing));
                        } else {
                            changeTask.future().complete(
                                    RuleChangeOutcome.first(processor.applyRuleChange(req)));
                        }
                    } catch (RuntimeException e) {
                        changeTask.future().completeExceptionally(e);
                    }
                } else if (task instanceof BlockTask blockTask) {
                    try {
                        Thread.sleep(blockTask.sleepMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
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

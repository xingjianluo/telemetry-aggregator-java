package com.github.xingjianluo.telemetryaggregatorjava.replay;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.dedup.EventDeduper;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.EventProcessor;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionMetrics;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.window.WatermarkGenerator;
import com.github.xingjianluo.telemetryaggregatorjava.window.WindowStore;
import jakarta.annotation.PreDestroy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 复盘服务：对指定事件时间边界重放确定性管道，重建该边界处已定稿的
 * 窗口聚合、生效规则版本、告警状态与调用链结论。
 *
 * <p>确定性：复盘只依赖事件日志（已提交结论）与规则变更历史；
 * 重放使用与实时完全相同的 {@link EventProcessor}，且告警状态机只使用事件时间，
 * 因此同一批输入无论何时、在哪台机器上复盘都得到同一结论。
 * 复盘在独立的状态副本上进行，绝不读写实时状态。
 *
 * <p>资源边界：单次复盘最多处理 {@code replay.max-events} 条事件；
 * 同时最多一个复盘在执行（其余快速失败 BUSY）；调用方等待有超时；
 * 超出事件日志 / 规则历史保留范围时显式返回 OUT_OF_RETENTION，
 * 绝不给出看似完整、实际缺数据的答案。
 */
public class ReplayService {

    private final TelemetryProperties props;
    private final EventJournal journal;
    private final RuleRegistry ruleRegistry;
    private final ExecutorService executor;
    private final AtomicBoolean busy = new AtomicBoolean();

    public ReplayService(TelemetryProperties props, EventJournal journal,
                         RuleRegistry ruleRegistry) {
        this.props = props;
        this.journal = journal;
        this.ruleRegistry = ruleRegistry;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "telemetry-replay");
            t.setDaemon(true);
            return t;
        });
    }

    /** 对事件时间边界 asOfMillis 做复盘。 */
    public ReplayReport replay(long asOfMillis) {
        if (asOfMillis < 0) {
            throw new IllegalArgumentException("asOfMillis 不能为负: " + asOfMillis);
        }
        // 保留范围前置检查：事件日志已淘汰的区间不可复盘。
        Long evicted = journal.oldestEvictedEventTimeMillis();
        if (evicted != null && asOfMillis >= evicted) {
            return new ReplayReport(ReplayStatus.OUT_OF_RETENTION,
                    "事件时间 <= " + evicted + " 的输入已超出复盘保留范围"
                            + "（telemetry.journal.max-events=" + props.getJournal().getMaxEvents()
                            + "，已淘汰 " + journal.evictedCount() + " 条），无法给出完整结论",
                    asOfMillis, Long.MIN_VALUE, List.of(), List.of(), List.of(), List.of(),
                    Map.of(), 0, 0);
        }
        List<EventJournal.Entry> entries = journal.snapshotUpTo(asOfMillis);
        if (entries.size() > props.getReplay().getMaxEvents()) {
            return new ReplayReport(ReplayStatus.TOO_LARGE,
                    "边界内事件量 " + entries.size() + " 超过单次复盘上限 "
                            + props.getReplay().getMaxEvents(),
                    asOfMillis, Long.MIN_VALUE, List.of(), List.of(), List.of(), List.of(),
                    Map.of(), 0, 0);
        }
        if (!busy.compareAndSet(false, true)) {
            return new ReplayReport(ReplayStatus.BUSY,
                    "已有复盘在执行，为保护实时摄取本次快速失败，请稍后重试",
                    asOfMillis, Long.MIN_VALUE, List.of(), List.of(), List.of(), List.of(),
                    Map.of(), 0, 0);
        }
        try {
            return executor.submit(() -> runReplay(asOfMillis, entries))
                    .get(props.getReplay().getTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return new ReplayReport(ReplayStatus.BUSY,
                    "复盘执行超时或被中断: " + e.getClass().getSimpleName(),
                    asOfMillis, Long.MIN_VALUE, List.of(), List.of(), List.of(), List.of(),
                    Map.of(), 0, 0);
        } finally {
            busy.set(false);
        }
    }

    private ReplayReport runReplay(long asOfMillis, List<EventJournal.Entry> entries) {
        // 全部使用全新状态副本：复盘不触碰任何实时组件。
        WatermarkGenerator watermark = new WatermarkGenerator(
                props.getWindow().getSizeMillis(),
                props.getWindow().getAllowedLatenessMillis(),
                false, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        WindowStore windowStore = new WindowStore(
                props.getCardinality().getMaxSeriesPerWindow(),
                props.getCardinality().getMaxSealedWindows());
        TraceRegistry traceRegistry = new TraceRegistry(
                props.getCardinality().getMaxTraces(),
                props.getCardinality().getMaxSpansPerTrace());
        AlertEngine alertEngine = new AlertEngine(
                props.getAlert().getBreachStreak(),
                props.getAlert().getRecoverStreak(),
                props.getAlert().getCooldown().toMillis(),
                props.getAlert().getEvidenceTrailSize());
        EventDeduper deduper = new EventDeduper(
                Math.max(props.getCardinality().getMaxEventIds(), entries.size() + 1));
        EventProcessor processor = new EventProcessor(props, watermark, windowStore,
                traceRegistry, alertEngine, ruleRegistry, deduper,
                new IngestionMetrics(), null);

        Map<RejectReason, Long> rejectedByReason = new EnumMap<>(RejectReason.class);
        long accepted = 0;
        for (EventJournal.Entry entry : entries) {
            if (entry.verdict().accepted()) {
                // 接受的事件按到达顺序重放：水位线推进、定稿与告警评估与实时逐位一致。
                processor.process(entry.event());
                accepted++;
            } else {
                // 确定拒绝的事件对状态无贡献，仅计入审计依据。
                rejectedByReason.merge(entry.verdict().rejectReason(), 1L, Long::sum);
            }
        }

        long replayWatermark = watermark.currentWatermark();
        List<WindowAggregate> aggregates = windowStore.query(null, null);

        // 规则历史保留范围检查：所有被评估窗口的 end 都必须晚于已淘汰的最老生效边界。
        Long ruleEvicted = ruleRegistry.oldestEvictedEffectiveFromMillis();
        if (ruleEvicted != null && !aggregates.isEmpty()) {
            long minWindowEnd = aggregates.stream()
                    .mapToLong(WindowAggregate::windowEndMillis).min().orElse(Long.MAX_VALUE);
            if (minWindowEnd <= ruleEvicted) {
                return new ReplayReport(ReplayStatus.OUT_OF_RETENTION,
                        "窗口 end=" + minWindowEnd + " 所需的规则版本已超出保留范围"
                                + "（telemetry.rules.max-versions=" + props.getRules().getMaxVersions()
                                + "），无法给出完整结论",
                        asOfMillis, replayWatermark, List.of(), List.of(), List.of(), List.of(),
                        Map.of(), 0, 0);
            }
        }

        // 边界内参照的规则版本历史（effectiveFrom <= asOf 即边界时刻已生效）。
        List<RuleChange> referenced = new ArrayList<>();
        for (RuleChange change : ruleRegistry.history()) {
            if (change.effectiveFromMillis() <= asOfMillis) {
                referenced.add(change);
            }
        }

        long sealedWindows = aggregates.stream()
                .map(WindowAggregate::windowStartMillis).distinct().count();
        return new ReplayReport(ReplayStatus.OK,
                "复盘完成：重放 " + entries.size() + " 条已提交事件（接受 " + accepted
                        + "，拒绝 " + (entries.size() - accepted) + "），水位线推进到 "
                        + replayWatermark,
                asOfMillis, replayWatermark,
                aggregates, alertEngine.snapshots(), traceRegistry.snapshotAll(),
                List.copyOf(referenced), Map.copyOf(rejectedByReason),
                accepted, sealedWindows);
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}

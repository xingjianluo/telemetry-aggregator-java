package com.github.xingjianluo.telemetryaggregatorjava.trace;

import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 调用链注册表：依据 spanId/parentSpanId 还原父子拓扑，并给出确定性的状态标记。
 *
 * <p>处理原则：孤立（parent 尚未到达或缺失）、乱序到达、形成有向环的链路都会被
 * <b>明确标记</b>（{@link TraceStatus#ORPHAN} / {@link TraceStatus#CYCLE} /
 * {@link TraceStatus#DUPLICATE_SPAN}），数据照常保留，告警判定不会中断。
 * 随着迟到/乱序 span 补齐，状态会重新评估（ORPHAN 可恢复为 COMPLETE；
 * CYCLE/DUPLICATE_SPAN 是结构性事实，不会被后续数据覆盖为 COMPLETE）。
 */
public class TraceRegistry {

    private static final class Trace {
        final Map<String, SpanInfo> spans = new LinkedHashMap<>();
        final Set<String> spanIds = new HashSet<>();
        boolean duplicateSpan;
        long firstEventTime = Long.MAX_VALUE;
        long lastEventTime = Long.MIN_VALUE;
        long lastTouchedMillis;

        boolean structuralAnomaly() {
            return duplicateSpan || !cycles().isEmpty();
        }

        /** Kahn 判定：返回成环边上的 spanId 集合；空集合表示无环。 */
        Set<String> cycles() {
            Map<String, Integer> indegree = new HashMap<>();
            Map<String, List<String>> children = new HashMap<>();
            for (SpanInfo s : spans.values()) {
                indegree.putIfAbsent(s.spanId(), 0);
                if (s.parentSpanId() != null && spans.containsKey(s.parentSpanId())) {
                    children.computeIfAbsent(s.parentSpanId(), k -> new ArrayList<>()).add(s.spanId());
                    indegree.merge(s.spanId(), 1, Integer::sum);
                }
            }
            List<String> queue = new ArrayList<>();
            for (Map.Entry<String, Integer> e : indegree.entrySet()) {
                if (e.getValue() == 0) {
                    queue.add(e.getKey());
                }
            }
            int visited = 0;
            while (!queue.isEmpty()) {
                String node = queue.remove(queue.size() - 1);
                visited++;
                for (String child : children.getOrDefault(node, List.of())) {
                    int d = indegree.merge(child, -1, Integer::sum);
                    if (d == 0) {
                        queue.add(child);
                    }
                }
            }
            if (visited == spans.size()) {
                return Set.of();
            }
            Set<String> cyclic = new LinkedHashSet<>();
            for (Map.Entry<String, Integer> e : indegree.entrySet()) {
                if (e.getValue() > 0) {
                    cyclic.add(e.getKey());
                }
            }
            return cyclic;
        }
    }

    private final Map<String, Trace> traces = new LinkedHashMap<>();
    private final int maxTraces;
    private final int maxSpansPerTrace;

    public TraceRegistry(int maxTraces, int maxSpansPerTrace) {
        this.maxTraces = maxTraces;
        this.maxSpansPerTrace = maxSpansPerTrace;
    }

    /** 注册事件中的 span；基数超限时返回明确拒绝原因（事件不进入任何统计）。 */
    public synchronized Optional<RejectReason> add(TelemetryEvent event) {
        String traceId = event.traceId();
        Trace trace = traces.get(traceId);
        if (trace == null) {
            if (traces.size() >= maxTraces) {
                return Optional.of(RejectReason.TOO_MANY_TRACES);
            }
            trace = new Trace();
            traces.put(traceId, trace);
        }
        SpanInfo span = new SpanInfo(event.spanId(), event.parentSpanId(),
                event.eventTimeMillis(), event.value());
        if (trace.spanIds.contains(event.spanId())) {
            trace.duplicateSpan = true;
        } else {
            if (trace.spans.size() >= maxSpansPerTrace) {
                return Optional.of(RejectReason.TOO_MANY_SPANS);
            }
            trace.spanIds.add(event.spanId());
            trace.spans.put(event.spanId(), span);
        }
        trace.firstEventTime = Math.min(trace.firstEventTime, event.eventTimeMillis());
        trace.lastEventTime = Math.max(trace.lastEventTime, event.eventTimeMillis());
        trace.lastTouchedMillis = System.currentTimeMillis();
        return Optional.empty();
    }

    /** 重新评估链路拓扑并产出快照与判定依据。 */
    public synchronized TraceSnapshot snapshot(String traceId) {
        Trace trace = traces.get(traceId);
        if (trace == null) {
            return null;
        }
        List<String> reasons = new ArrayList<>();
        List<String> orphans = new ArrayList<>();
        Set<String> roots = new LinkedHashSet<>();
        for (SpanInfo s : trace.spans.values()) {
            if (s.parentSpanId() == null || s.parentSpanId().isBlank()) {
                roots.add(s.spanId());
            } else if (!trace.spans.containsKey(s.parentSpanId())) {
                orphans.add(s.spanId() + "->缺失parent:" + s.parentSpanId());
            }
        }
        Set<String> cyclic = trace.cycles();

        TraceStatus status;
        if (trace.duplicateSpan) {
            status = TraceStatus.DUPLICATE_SPAN;
            reasons.add("检测到相同 spanId 被重复投递，链路内 span 标识冲突");
        } else if (!cyclic.isEmpty()) {
            status = TraceStatus.CYCLE;
            reasons.add("父子边构成有向环，涉及 span: " + cyclic);
        } else if (!orphans.isEmpty() || roots.size() != 1) {
            status = TraceStatus.ORPHAN;
            if (!orphans.isEmpty()) {
                reasons.add("存在孤立 span（parent 尚未到达或不存在）: " + orphans
                        + "；乱序到达时补齐后可重新评估");
            }
            if (roots.isEmpty()) {
                reasons.add("链路无根 span（可能全部位于环上或 parent 均缺失）");
            } else if (roots.size() > 1) {
                reasons.add("存在多个根 span: " + roots);
            }
        } else {
            status = TraceStatus.COMPLETE;
            reasons.add("唯一根 span=" + roots.iterator().next() + "，全部 span 连通且无环");
        }
        List<SpanInfo> spans = List.copyOf(trace.spans.values());
        return new TraceSnapshot(traceId, status, spans, reasons,
                trace.firstEventTime == Long.MAX_VALUE ? -1L : trace.firstEventTime,
                trace.lastEventTime == Long.MIN_VALUE ? -1L : trace.lastEventTime);
    }

    /** 回收超过保留期未再更新的调用链，保证内存有界。 */
    public synchronized void pruneIdle(long nowMillis, long retentionMillis) {
        traces.entrySet().removeIf(e -> nowMillis - e.getValue().lastTouchedMillis > retentionMillis);
    }

    public synchronized int traceCount() {
        return traces.size();
    }
}

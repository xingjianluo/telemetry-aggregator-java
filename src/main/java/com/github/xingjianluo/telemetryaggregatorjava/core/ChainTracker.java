package com.github.xingjianluo.telemetryaggregatorjava.core;

import com.github.xingjianluo.telemetryaggregatorjava.model.ChainStatus;
import com.github.xingjianluo.telemetryaggregatorjava.model.ChainView;
import com.github.xingjianluo.telemetryaggregatorjava.model.SpanNode;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 依据 {@code traceId / spanId / parentSpanId} 还原调用链（单线程访问）。
 *
 * <p>规则：</p>
 * <ul>
 *   <li>span 可乱序到达：父 span 未到时子 span 标记 {@code AWAITING_PARENT}，
 *       父 span 到达后自动升级；链路永不丢弃，判定不中断。</li>
 *   <li>同一 traceId 下 spanId 重复但负载不同 → {@code conflict}（标识冲突），
 *       不覆盖已记录的节点，按 {@code ID_CONFLICT} 拒绝。</li>
 *   <li>父引用形成有向环（含自引用）→ {@code CYCLE}，并给出环上 spanId 序列。</li>
 *   <li>随水位线剪枝时：完整且整体早于水位线的链路删除；仍不完整的链路改标
 *       {@code DISCONNECTED}（父已不可能到达）但保留以供查询。</li>
 *   <li>同一 traceId 有多个根 → DISCONNECTED 风格的多根异常，通过状态标记。</li>
 * </ul>
 */
public final class ChainTracker {

    /** addSpan 结果。 */
    public static final class AddOutcome {
        private final boolean accepted;
        private final boolean conflict;
        private final String reason;
        private final ChainView view;

        public AddOutcome(boolean accepted, boolean conflict, String reason, ChainView view) {
            this.accepted = accepted;
            this.conflict = conflict;
            this.reason = reason;
            this.view = view;
        }

        public boolean isAccepted() {
            return accepted;
        }

        public boolean isConflict() {
            return conflict;
        }

        public String getReason() {
            return reason;
        }

        public ChainView getView() {
            return view;
        }
    }

    /** 一条 trace 的内部状态。 */
    private static final class Trace {
        final String traceId;
        final Map<String, SpanNode> spans = new LinkedHashMap<>();
        ChainStatus override; // 剪枝时被标记为 DISCONNECTED 后锁定
        boolean everComplete;

        Trace(String traceId) {
            this.traceId = traceId;
        }
    }

    private final int maxSpans;
    private final Map<String, Trace> traces = new LinkedHashMap<>();
    private int spanCount = 0;

    public ChainTracker(int maxSpans) {
        this.maxSpans = maxSpans;
    }

    public AddOutcome addSpan(TelemetryEvent event) {
        String traceId = event.getTraceId();
        Trace trace = traces.computeIfAbsent(traceId, Trace::new);

        SpanNode existing = trace.spans.get(event.getSpanId());
        if (existing != null) {
            if (!sameSpan(existing, event)) {
                return new AddOutcome(false, true,
                        "spanId '" + event.getSpanId() + "' already exists in trace '"
                                + traceId + "' with different payload "
                                + "(parent/duration/eventTime differ) -> ID_CONFLICT",
                        snapshot(trace));
            }
            // 完全相同的 span 重复投递：视为幂等（去重表通常已拦截），返回当前视图
            return new AddOutcome(true, false,
                    "identical span already present; idempotent replay", snapshot(trace));
        }

        if (spanCount >= maxSpans) {
            return new AddOutcome(false, false,
                    "global span cardinality limit " + maxSpans
                            + " reached -> TAG_CARDINALITY", snapshot(trace));
        }

        SpanNode node = new SpanNode(event.getSpanId(),
                emptyToNull(event.getParentSpanId()),
                event.getDurationMillis(), event.getTimestampMillis(), event.getEventId());
        trace.spans.put(node.getSpanId(), node);
        spanCount++;
        trace.everComplete = false; // 结构可能改变，重算时处理
        return new AddOutcome(true, false, "span recorded", snapshot(trace));
    }

    private static boolean sameSpan(SpanNode n, TelemetryEvent e) {
        String parent = emptyToNull(e.getParentSpanId());
        boolean sameParent = n.getParentSpanId() == null
                ? parent == null : n.getParentSpanId().equals(parent);
        return sameParent
                && n.getDurationMillis() == e.getDurationMillis()
                && n.getTimestampMillis() == e.getTimestampMillis();
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }

    private ChainView snapshot(Trace trace) {
        // 环检测
        List<String> cycle = findCycle(trace);
        ChainStatus status;
        long totalDuration = 0;
        int depth = 0;

        if (trace.override != null) {
            status = trace.override;
        } else if (!cycle.isEmpty()) {
            status = ChainStatus.CYCLE;
        } else {
            Set<String> missingParents = new HashSet<>();
            int roots = 0;
            for (SpanNode n : trace.spans.values()) {
                if (n.getParentSpanId() == null) {
                    roots++;
                } else if (!trace.spans.containsKey(n.getParentSpanId())) {
                    missingParents.add(n.getParentSpanId());
                }
            }
            if (roots == 0) {
                // 无环却没有根：所有 span 都挂在缺失父之下（被剪枝的孤立链）
                status = missingParents.isEmpty() ? ChainStatus.CYCLE : ChainStatus.AWAITING_PARENT;
            } else if (!missingParents.isEmpty()) {
                status = ChainStatus.AWAITING_PARENT;
            } else if (roots > 1) {
                status = ChainStatus.DISCONNECTED;
            } else {
                status = ChainStatus.COMPLETE;
                trace.everComplete = true;
            }
        }

        for (SpanNode n : trace.spans.values()) {
            totalDuration += n.getDurationMillis();
        }
        if (cycle.isEmpty()) {
            depth = computeDepth(trace);
        }

        List<SpanNode> nodes = new ArrayList<>(trace.spans.values());
        long updatedAt = nodes.stream().mapToLong(SpanNode::getTimestampMillis).max().orElse(0L);
        return new ChainView(trace.traceId, status, cycle, nodes,
                totalDuration, depth, updatedAt);
    }

    /**
     * DFS 三色遍历查找父引用环；返回环上的 spanId（沿 parent 指针），无环返回空列表。
     */
    private List<String> findCycle(Trace trace) {
        Map<String, Integer> color = new LinkedHashMap<>(); // 0=white 1=gray 2=black
        List<String> cyclePath = new ArrayList<>();
        for (String startId : trace.spans.keySet()) {
            if (color.getOrDefault(startId, 0) != 0) {
                continue;
            }
            List<String> path = new ArrayList<>();
            String cur = startId;
            while (cur != null && trace.spans.containsKey(cur)) {
                int c = color.getOrDefault(cur, 0);
                if (c == 1) {
                    int idx = path.indexOf(cur);
                    cyclePath.addAll(path.subList(idx, path.size()));
                    cyclePath.add(cur);
                    return cyclePath;
                }
                if (c == 2) {
                    break;
                }
                color.put(cur, 1);
                path.add(cur);
                cur = trace.spans.get(cur).getParentSpanId();
            }
            for (String p : path) {
                color.put(p, 2);
            }
        }
        return cyclePath;
    }

    /** 计算最大深度：根深度为 1；对缺失父的节点以其自身为根计算（不让判定中断）。 */
    private int computeDepth(Trace trace) {
        Map<String, Integer> memo = new LinkedHashMap<>();
        int max = 0;
        for (String id : trace.spans.keySet()) {
            max = Math.max(max, depthOf(id, trace, memo, new HashSet<>()));
        }
        return max;
    }

    private int depthOf(String id, Trace trace, Map<String, Integer> memo, Set<String> onPath) {
        Integer cached = memo.get(id);
        if (cached != null) {
            return cached;
        }
        if (!onPath.add(id)) {
            return 1; // 环的保险分支（环链通常不调用深度）
        }
        SpanNode n = trace.spans.get(id);
        int depth;
        if (n.getParentSpanId() == null || !trace.spans.containsKey(n.getParentSpanId())) {
            depth = 1;
        } else {
            depth = 1 + depthOf(n.getParentSpanId(), trace, memo, onPath);
        }
        onPath.remove(id);
        memo.put(id, depth);
        return depth;
    }

    public ChainView getChain(String traceId) {
        Trace trace = traces.get(traceId);
        return trace == null ? null : snapshot(trace);
    }

    public List<ChainView> allChains() {
        List<ChainView> out = new ArrayList<>();
        for (Trace t : traces.values()) {
            out.add(snapshot(t));
        }
        return out;
    }

    /**
     * 随水位线剪枝：
     * 完整且最大事件时间 &lt;= watermark - windowMillis 的链路视为已过窗，删除；
     * 不完整（等待父）且最旧 span 已早于水位线的链路标记 DISCONNECTED 后保留。
     * 返回删除的链路数。
     */
    public int prune(long watermark, long windowMillis) {
        int removed = 0;
        var iter = traces.entrySet().iterator();
        while (iter.hasNext()) {
            Trace t = iter.next().getValue();
            ChainView view = snapshot(t);
            long latest = view.getUpdatedAtEventTime();
            if (view.getStatus() == ChainStatus.COMPLETE
                    && latest + windowMillis <= watermark) {
                spanCount -= t.spans.size();
                iter.remove();
                removed++;
            } else if (t.override == null
                    && (view.getStatus() == ChainStatus.AWAITING_PARENT)) {
                long oldest = t.spans.values().stream()
                        .mapToLong(SpanNode::getTimestampMillis).min().orElse(Long.MAX_VALUE);
                if (oldest < watermark) {
                    t.override = ChainStatus.DISCONNECTED;
                }
            }
        }
        return removed;
    }

    public int spanCount() {
        return spanCount;
    }

    /**
     * 回滚由 addSpan 刚刚写入、但随后在窗口基数校验中被拒绝的 span。
     * 仅当该节点确实存在时删除。由单工作线程调用。
     */
    public void rollbackLastSpan(String traceId, String spanId) {
        Trace trace = traces.get(traceId);
        if (trace != null && trace.spans.remove(spanId) != null) {
            spanCount--;
            if (trace.spans.isEmpty() && trace.override == null) {
                traces.remove(traceId);
            }
        }
    }
}

package com.github.xingjianluo.telemetryaggregatorjava.core;

import com.github.xingjianluo.telemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.telemetryaggregatorjava.model.SeriesAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowResult;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 按事件时间的滚动窗口表（单线程访问）。
 *
 * <p>窗口归属规则（确定可复现）：</p>
 * <pre>
 *   windowStart = floor(eventTime / windowMillis) * windowMillis
 *   windowEnd   = windowStart + windowMillis
 * </pre>
 * <p>水位线 W 推进后，所有 {@code windowEnd <= W} 的窗口按起点升序被关闭，
 * 产出不可变 {@link WindowResult}。被拒绝（迟到/基数）的事件从不进入窗口，
 * 因此关闭后的结论不受后续重试影响。</p>
 *
 * <p>有界保护（拒绝均带可区分原因，不静默丢弃）：</p>
 * <ul>
 *   <li>活动窗口数超过 {@code maxActiveWindows} → TOO_MANY_ACTIVE_WINDOWS；</li>
 *   <li>单窗口内某 tag key 的不同 value 数超限 → TAG_CARDINALITY（reason 注明 key）；</li>
 *   <li>单窗口 series 数超限 → TAG_CARDINALITY（reason 注明 series 上限）。</li>
 * </ul>
 * <p>所有基数检查在任何状态修改之前完成，因此拒绝不留下部分写入。</p>
 */
public final class WindowTable {

    /** applyEvent 的结果。 */
    public static final class ApplyOutcome {
        private final IngestStatus status;
        private final String reason;
        private final Long windowStart;

        public ApplyOutcome(IngestStatus status, String reason, Long windowStart) {
            this.status = status;
            this.reason = reason;
            this.windowStart = windowStart;
        }

        public IngestStatus getStatus() {
            return status;
        }

        public String getReason() {
            return reason;
        }

        public Long getWindowStart() {
            return windowStart;
        }
    }

    /** 单个活动窗口的可变内部状态。 */
    private static final class WindowState {
        final long start;
        final long end;
        // seriesKey -> 聚合
        final Map<String, SeriesAggregate> series = new TreeMap<>();
        // 标签基数：tagKey -> 该窗口内出现过的 value 集合
        final Map<String, Set<String>> tagValues = new TreeMap<>();

        WindowState(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }

    private final long windowMillis;
    private final int maxTagValuesPerKey;
    private final int maxSeriesPerWindow;
    private final int maxActiveWindows;
    private final int retainedWindows;

    private final TreeMap<Long, WindowState> active = new TreeMap<>();
    private final ArrayDeque<WindowResult> closed = new ArrayDeque<>();

    public WindowTable(long windowMillis, int maxTagValuesPerKey, int maxSeriesPerWindow,
                       int maxActiveWindows, int retainedWindows) {
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be > 0");
        }
        this.windowMillis = windowMillis;
        this.maxTagValuesPerKey = maxTagValuesPerKey;
        this.maxSeriesPerWindow = maxSeriesPerWindow;
        this.maxActiveWindows = maxActiveWindows;
        this.retainedWindows = retainedWindows;
    }

    public long windowStartOf(long eventTimeMillis) {
        return Math.floorDiv(eventTimeMillis, windowMillis) * windowMillis;
    }

    /**
     * 将已通过去重与水位线校验的事件计入窗口。
     */
    public ApplyOutcome applyEvent(TelemetryEvent event, long watermark) {
        long start = windowStartOf(event.getTimestampMillis());

        WindowState window = active.get(start);
        if (window == null) {
            if (active.size() >= maxActiveWindows) {
                return new ApplyOutcome(IngestStatus.TOO_MANY_ACTIVE_WINDOWS,
                        "active window count limit " + maxActiveWindows
                                + " reached; event for window starting at " + start
                                + " rejected before creating new window", start);
            }
            window = new WindowState(start, start + windowMillis);
            active.put(start, window);
        }

        TreeMap<String, String> tags = event.getTags() == null
                ? new TreeMap<>() : new TreeMap<>(event.getTags());
        String seriesKey = seriesKey(event.getMetric(), tags);

        boolean newSeries = !window.series.containsKey(seriesKey);
        if (newSeries) {
            if (window.series.size() >= maxSeriesPerWindow) {
                return new ApplyOutcome(IngestStatus.TAG_CARDINALITY,
                        "series cardinality limit " + maxSeriesPerWindow
                                + " reached in window [" + start + "," + window.end + ")",
                        start);
            }
            // 先校验所有 tag 的基数，任一超限即整体拒绝，绝不部分写入
            for (Map.Entry<String, String> t : tags.entrySet()) {
                Set<String> values = window.tagValues.get(t.getKey());
                if (values != null && !values.contains(t.getValue())
                        && values.size() >= maxTagValuesPerKey) {
                    return new ApplyOutcome(IngestStatus.TAG_CARDINALITY,
                            "tag key '" + t.getKey() + "' distinct value limit "
                                    + maxTagValuesPerKey
                                    + " reached in window [" + start + "," + window.end + ")",
                            start);
                }
            }
        }

        // 校验通过，执行写入
        if (newSeries) {
            for (Map.Entry<String, String> t : tags.entrySet()) {
                window.tagValues.computeIfAbsent(t.getKey(), k -> new HashSet<>())
                        .add(t.getValue());
            }
            window.series.put(seriesKey, new SeriesAggregate(event.getMetric(), tags));
        }
        window.series.get(seriesKey).add(event.getValue());
        return new ApplyOutcome(IngestStatus.ACCEPTED,
                "counted in window [" + start + "," + window.end + ")", start);
    }

    private static String seriesKey(String metric, TreeMap<String, String> tags) {
        StringBuilder sb = new StringBuilder(metric).append('|');
        tags.forEach((k, v) -> sb.append(k).append('=').append(v).append(';'));
        return sb.toString();
    }

    /**
     * 关闭所有 {@code end <= watermark} 的窗口（按起点升序），产出不可变快照。
     */
    public List<WindowResult> closeUpTo(long watermark) {
        List<WindowResult> out = new ArrayList<>();
        while (!active.isEmpty()) {
            Map.Entry<Long, WindowState> first = active.firstEntry();
            WindowState w = first.getValue();
            if (w.end > watermark) {
                break;
            }
            active.pollFirstEntry();
            List<SeriesAggregate> snapshot = new ArrayList<>(w.series.size());
            w.series.values().forEach(a -> snapshot.add(new SeriesAggregate(a)));
            WindowResult result = new WindowResult(w.start, w.end, watermark, snapshot);
            closed.addLast(result);
            while (closed.size() > retainedWindows) {
                closed.pollFirst();
            }
            out.add(result);
        }
        return out;
    }

    public int activeWindowCount() {
        return active.size();
    }

    /** 已关闭窗口：返回窗口起点位于 [from,to) 的结果（升序）；null 表示不限制。 */
    public List<WindowResult> closedWindows(Long from, Long to) {
        List<WindowResult> out = new ArrayList<>();
        for (WindowResult r : closed) {
            if (from != null && r.getWindowStart() < from) {
                continue;
            }
            if (to != null && r.getWindowStart() >= to) {
                continue;
            }
            out.add(r);
        }
        return out;
    }

    public List<WindowResult> allClosedWindows() {
        return Collections.unmodifiableList(new ArrayList<>(closed));
    }
}

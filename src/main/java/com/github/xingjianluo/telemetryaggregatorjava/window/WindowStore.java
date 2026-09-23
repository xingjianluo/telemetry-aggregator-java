package com.github.xingjianluo.telemetryaggregatorjava.window;

import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事件时间窗口聚合存储。
 *
 * <p>结构：windowStart -&gt; （seriesKey -&gt; 累加器），另有一套独立的
 * trace × service × metric 累加器供调用链作用域告警使用（不污染标签基数）。
 * 计数由单 worker 线程驱动；累加器自身同步，保证并发查询读到一致快照。
 *
 * <p>基数保护：每个窗口内不同标签序列数达到 {@code maxSeriesPerWindow} 即拒绝新序列；
 * trace 序列按独立上限保护；已关闭窗口结果保留 {@code maxSealedWindows} 个，
 * 超出时丢弃最老窗口并计数（可观测），绝不静默。
 */
public class WindowStore {

    /** 单个序列的可变累加器。 */
    static final class Accumulator {
        long count;
        double sum;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        synchronized void add(double v) {
            count++;
            sum += v;
            if (v < min) {
                min = v;
            }
            if (v > max) {
                max = v;
            }
        }

        synchronized long count() {
            return count;
        }

        /** 补偿回滚最近一次 add（仅在同一事件后续计入失败时由 worker 调用）。 */
        synchronized void rollback(double v) {
            if (count == 0) {
                return;
            }
            count--;
            sum -= v;
            if (count == 0) {
                min = Double.POSITIVE_INFINITY;
                max = Double.NEGATIVE_INFINITY;
            }
        }
    }

    private record SeriesKey(String service, String metric, String tagsSignature, Map<String, String> tags) {
    }

    /** trace 作用域的聚合键：窗口 × trace × service × metric。 */
    private record TraceKey(long windowStart, String traceId, String service, String metric) {
    }

    /** trace 窗口聚合定稿结果。 */
    public record TraceAggregate(long windowStartMillis, long windowEndMillis, String traceId,
                                 String service, String metric, long count, double sum, double avg) {
    }

    private final Map<Long, Map<SeriesKey, Accumulator>> live = new ConcurrentHashMap<>();
    private final Map<Long, Map<TraceKey, Accumulator>> liveTrace = new ConcurrentHashMap<>();
    private final TreeMap<Long, List<WindowAggregate>> sealed = new TreeMap<>();
    private final TreeMap<Long, List<TraceAggregate>> sealedTrace = new TreeMap<>();

    private final int maxSeriesPerWindow;
    private final int maxSealedWindows;

    private long droppedSealedWindows;
    private long droppedSealedAggregates;

    public WindowStore(int maxSeriesPerWindow, int maxSealedWindows) {
        this.maxSeriesPerWindow = maxSeriesPerWindow;
        this.maxSealedWindows = maxSealedWindows;
    }

    /**
     * 将事件计入其标签窗口。返回非空原因表示因基数上限被拒绝（未产生任何计数）。
     */
    public Optional<RejectReason> add(TelemetryEvent event, long windowStartMillis) {
        Map<SeriesKey, Accumulator> window =
                live.computeIfAbsent(windowStartMillis, k -> new ConcurrentHashMap<>());
        SeriesKey key = new SeriesKey(
                nullToEmpty(event.service()),
                nullToEmpty(event.metric()),
                tagsSignature(event.tags()),
                sortedCopy(event.tags()));
        Accumulator acc = window.get(key);
        if (acc == null) {
            if (window.size() >= maxSeriesPerWindow) {
                return Optional.of(RejectReason.TOO_MANY_SERIES);
            }
            acc = new Accumulator();
            Accumulator raced = window.putIfAbsent(key, acc);
            if (raced != null) {
                acc = raced;
            } else if (window.size() > maxSeriesPerWindow) {
                // 并发创建时瞬时超额：回滚本次创建并拒绝，绝不静默计入。
                window.remove(key, acc);
                return Optional.of(RejectReason.TOO_MANY_SERIES);
            }
        }
        acc.add(event.value());
        return Optional.empty();
    }

    /** 补偿回滚：把该事件从标签窗口累加器中撤销（后续计入失败时保证原子性）。 */
    public void removeLast(TelemetryEvent event, long windowStartMillis) {
        Map<SeriesKey, Accumulator> window = live.get(windowStartMillis);
        if (window == null) {
            return;
        }
        SeriesKey key = new SeriesKey(
                nullToEmpty(event.service()), nullToEmpty(event.metric()),
                tagsSignature(event.tags()), sortedCopy(event.tags()));
        Accumulator acc = window.get(key);
        if (acc != null) {
            acc.rollback(event.value());
            if (acc.count() == 0) {
                window.remove(key, acc);
            }
        }
    }

    /**
     * 将事件计入其 trace × 窗口累加器。基数上限按“每窗口 trace 序列数”独立计算，
     * 与标签序列基数互不占用；超限返回明确原因。
     */
    public Optional<RejectReason> addTrace(TelemetryEvent event, long windowStartMillis,
                                           int maxTraceSeriesPerWindow) {
        Map<TraceKey, Accumulator> window =
                liveTrace.computeIfAbsent(windowStartMillis, k -> new ConcurrentHashMap<>());
        TraceKey key = new TraceKey(windowStartMillis, event.traceId(),
                nullToEmpty(event.service()), nullToEmpty(event.metric()));
        Accumulator acc = window.get(key);
        if (acc == null) {
            if (window.size() >= maxTraceSeriesPerWindow) {
                return Optional.of(RejectReason.TOO_MANY_SERIES);
            }
            acc = new Accumulator();
            Accumulator raced = window.putIfAbsent(key, acc);
            if (raced != null) {
                acc = raced;
            } else if (window.size() > maxTraceSeriesPerWindow) {
                window.remove(key, acc);
                return Optional.of(RejectReason.TOO_MANY_SERIES);
            }
        }
        acc.add(event.value());
        return Optional.empty();
    }

    /** 补偿回滚：把该事件从 trace × 窗口累加器中撤销。 */
    public void removeLastTrace(TelemetryEvent event, long windowStartMillis) {
        Map<TraceKey, Accumulator> window = liveTrace.get(windowStartMillis);
        if (window == null) {
            return;
        }
        TraceKey key = new TraceKey(windowStartMillis, event.traceId(),
                nullToEmpty(event.service()), nullToEmpty(event.metric()));
        Accumulator acc = window.get(key);
        if (acc != null) {
            acc.rollback(event.value());
            if (acc.count() == 0) {
                window.remove(key, acc);
            }
        }
    }

    /** 关闭所有 end &lt;= watermark 的窗口，定稿并转移结果。 */
    public synchronized List<WindowAggregate> sealUpTo(long watermarkMillis, long windowSizeMillis) {
        List<WindowAggregate> output = new ArrayList<>();
        List<Long> sealedStarts = new ArrayList<>();
        for (Long start : new ArrayList<>(live.keySet())) {
            if (!WatermarkGenerator.sealable(start, windowSizeMillis, watermarkMillis)) {
                continue;
            }
            Map<SeriesKey, Accumulator> w = live.get(start);
            if (w == null) {
                continue;
            }
            sealedStarts.add(start);
            long end = WatermarkGenerator.windowEnd(start, windowSizeMillis);
            for (Map.Entry<SeriesKey, Accumulator> s : w.entrySet()) {
                SeriesKey k = s.getKey();
                Accumulator a = s.getValue();
                long count = a.count();
                output.add(new WindowAggregate(
                        start, end, k.service(), k.metric(), k.tags(),
                        count, a.sum, a.min, a.max, count));
            }
        }
        if (sealedStarts.isEmpty()) {
            return List.of();
        }
        sealedStarts.forEach(start -> {
            live.remove(start);
            Map<TraceKey, Accumulator> traceWindow = liveTrace.remove(start);
            if (traceWindow != null) {
                long end = WatermarkGenerator.windowEnd(start, windowSizeMillis);
                List<TraceAggregate> traceList = new ArrayList<>();
                for (Map.Entry<TraceKey, Accumulator> e : traceWindow.entrySet()) {
                    TraceKey k = e.getKey();
                    Accumulator a = e.getValue();
                    long count = a.count();
                    traceList.add(new TraceAggregate(start, end, k.traceId(), k.service(),
                            k.metric(), count, a.sum, count == 0 ? 0.0 : a.sum / count));
                }
                traceList.sort(Comparator.comparing(TraceAggregate::traceId)
                        .thenComparing(TraceAggregate::service)
                        .thenComparing(TraceAggregate::metric));
                sealedTrace.put(start, traceList);
            }
        });

        Map<Long, List<WindowAggregate>> byStart = new TreeMap<>();
        for (WindowAggregate a : output) {
            byStart.computeIfAbsent(a.windowStartMillis(), k -> new ArrayList<>()).add(a);
        }
        byStart.forEach((start, list) -> {
            list.sort(Comparator.comparing(WindowAggregate::service)
                    .thenComparing(WindowAggregate::metric)
                    .thenComparing(w -> w.tags().toString()));
            sealed.put(start, list);
        });
        enforceSealedBound();
        output.sort(Comparator.comparingLong(WindowAggregate::windowStartMillis)
                .thenComparing(WindowAggregate::service)
                .thenComparing(WindowAggregate::metric));
        return output;
    }

    private void enforceSealedBound() {
        while (sealed.size() > maxSealedWindows) {
            Map.Entry<Long, List<WindowAggregate>> oldest = sealed.pollFirstEntry();
            if (oldest == null) {
                break;
            }
            droppedSealedWindows++;
            droppedSealedAggregates += oldest.getValue().size();
            sealedTrace.remove(oldest.getKey());
        }
        while (sealedTrace.size() > maxSealedWindows) {
            sealedTrace.pollFirstEntry();
        }
    }

    /** 取出某已关闭窗口的 trace 聚合（用于告警评估）。 */
    public synchronized List<TraceAggregate> sealedTraceAggregates(long windowStartMillis) {
        return sealedTrace.getOrDefault(windowStartMillis, List.of());
    }

    /** 查询已关闭窗口的定稿聚合（与告警评估使用同一份结果）。 */
    public synchronized List<WindowAggregate> query(String service, String metric) {
        List<WindowAggregate> result = new ArrayList<>();
        for (List<WindowAggregate> list : sealed.values()) {
            for (WindowAggregate a : list) {
                if (service != null && !service.equals(a.service())) {
                    continue;
                }
                if (metric != null && !metric.equals(a.metric())) {
                    continue;
                }
                result.add(a);
            }
        }
        return List.copyOf(result);
    }

    public int liveWindowCount() {
        return live.size();
    }

    public int liveSeriesCount() {
        int total = 0;
        for (Map<SeriesKey, Accumulator> w : live.values()) {
            total += w.size();
        }
        return total;
    }

    public synchronized int sealedWindowCount() {
        return sealed.size();
    }

    public synchronized long droppedSealedWindows() {
        return droppedSealedWindows;
    }

    public synchronized long droppedSealedAggregates() {
        return droppedSealedAggregates;
    }

    public static String tagsSignature(Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : new TreeMap<>(tags).entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.append('}').toString();
    }

    private static Map<String, String> sortedCopy(Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return Map.of();
        }
        return Map.copyOf(new TreeMap<>(tags));
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}

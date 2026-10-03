package com.github.xingjianluo.telemetryaggregatorjava.replay;

import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * 事件日志：按到达顺序记录每条已提交结论（接受或确定性拒绝）的事件，
 * 是复盘的唯一事件来源，保证复盘只由输入事件与规则变更历史决定。
 *
 * <p>容量有界：超过 {@code maxEvents} 时按事件时间最老的条目淘汰，
 * 并记录被淘汰的最老事件时间——复盘边界触及已淘汰区间时，
 * 复盘服务必须显式拒绝（OUT_OF_RETENTION），绝不给出缺数据的“完整”答案。
 *
 * <p>线程安全：实时 worker 追加与复盘快照拷贝可并发。
 */
public class EventJournal {

    /** 一条已提交结论：到达序号 + 事件 + 首次判定。 */
    public record Entry(long seq, TelemetryEvent event, IngestResult verdict) {
    }

    private static final Comparator<Entry> BY_EVENT_TIME =
            Comparator.comparingLong((Entry e) -> e.event().eventTimeMillis())
                    .thenComparingLong(Entry::seq);

    private final TreeSet<Entry> byEventTime = new TreeSet<>(BY_EVENT_TIME);
    private final int maxEvents;
    private long nextSeq;
    private Long oldestEvictedEventTime;
    private long evictedCount;

    public EventJournal(int maxEvents) {
        this.maxEvents = Math.max(1, maxEvents);
    }

    /** 追加一条已提交结论（worker 单线程调用）。 */
    public synchronized void append(TelemetryEvent event, IngestResult verdict) {
        byEventTime.add(new Entry(nextSeq++, event, verdict));
        while (byEventTime.size() > maxEvents) {
            Entry evicted = byEventTime.pollFirst();
            if (evicted != null && oldestEvictedEventTime == null) {
                // 淘汰始终取当前最小事件时间，被淘汰的事件时间序列单调不减，
                // 首个被淘汰者即“可复盘下界”。
                oldestEvictedEventTime = evicted.event().eventTimeMillis();
            }
            evictedCount++;
        }
    }

    /** 事件时间不晚于 asOfMillis 的全部条目，按到达（seq）顺序返回快照。 */
    public synchronized List<Entry> snapshotUpTo(long asOfMillis) {
        List<Entry> out = new ArrayList<>();
        for (Entry e : byEventTime) {
            if (e.event().eventTimeMillis() > asOfMillis) {
                break;
            }
            out.add(e);
        }
        out.sort(Comparator.comparingLong(Entry::seq));
        return out;
    }

    /** 因容量被淘汰的最老事件时间；无淘汰返回 null（复盘下界，不含该值）。 */
    public synchronized Long oldestEvictedEventTimeMillis() {
        return oldestEvictedEventTime;
    }

    public synchronized long evictedCount() {
        return evictedCount;
    }

    public synchronized int size() {
        return byEventTime.size();
    }
}

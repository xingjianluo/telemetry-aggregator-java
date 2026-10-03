package com.github.xingjianluo.telemetryaggregatorjava.replay;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertFact;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * 复盘日志：线性化事件、规则变更、告警判定事实的有界 append-only 记录。
 *
 * <p>所有 append 都在摄取 worker 单一线性化点发生（复用同一序列号），
 * 因此“变更与事件写入同时发生”天然有序、无丢失、无重复。
 *
 * <p>容量有界：事件与告警事实使用定长环形缓冲，超出丢弃最老条目并推进可复盘下界；
 * 规则变更历史不老化（运营频率天然有界，且缺了它旧边界就无法解释）。
 * 复盘早于保留下界的请求会被明确判为 NOT_REPLAYABLE，绝不返回缺数据的“完整”答案。
 */
public class EventJournal {

    private final int maxEvents;
    private final int maxFacts;

    private final Deque<JournaledEvent> events = new ArrayDeque<>();
    private final Deque<JournaledRuleChange> ruleChanges = new ArrayDeque<>();
    private final Deque<AlertFact> alertFacts = new ArrayDeque<>();

    /** 事件老化时保存的最新检查点（水位线单调推进，覆盖即可）。 */
    private AlertCheckpoint checkpoint;

    private long seqCounter = 0L;
    private long droppedEvents = 0L;
    private long droppedFacts = 0L;

    public EventJournal(int maxEvents, int maxFacts) {
        if (maxEvents <= 0 || maxFacts <= 0) {
            throw new IllegalArgumentException("journal capacity must be positive");
        }
        this.maxEvents = maxEvents;
        this.maxFacts = maxFacts;
    }

    /** 取下一个线性化序列号（事件与规则变更共用，严格递增）。 */
    public synchronized long nextSeq() {
        return ++seqCounter;
    }

    /** 当前（不递增）序列号。 */
    public synchronized long currentSeq() {
        return seqCounter;
    }

    /** 追加一条已接受事件（由 worker 在事件成功计入后调用）；超容量丢弃最老事件。 */
    public synchronized void appendEvent(long seq, TelemetryEvent event,
                                         long windowStartMillis, long watermarkAfterMillis) {
        events.addLast(new JournaledEvent(seq, event, windowStartMillis, watermarkAfterMillis));
        while (events.size() > maxEvents) {
            events.pollFirst();
            droppedEvents++;
        }
    }

    /** 追加一条规则变更（不老化）。 */
    public synchronized void appendRuleChange(long seq, RuleVersion version, long watermarkAtApplyMillis) {
        ruleChanges.addLast(new JournaledRuleChange(seq, version, watermarkAtApplyMillis));
    }

    /** 追加一条告警判定事实；超容量丢弃最老事实（与事件老化各自独立计数）。 */
    public synchronized void appendAlertFact(AlertFact fact) {
        alertFacts.addLast(fact);
        while (alertFacts.size() > maxFacts) {
            alertFacts.pollFirst();
            droppedFacts++;
        }
    }

    /** 按线性化顺序读取全部仍保留的事件（复盘重放用，返回快照副本）。 */
    public synchronized List<JournaledEvent> events() {
        return new ArrayList<>(events);
    }

    /** 按线性化顺序读取全部规则变更。 */
    public synchronized List<JournaledRuleChange> ruleChanges() {
        return new ArrayList<>(ruleChanges);
    }

    /** 按线性化顺序读取全部仍保留的告警判定事实。 */
    public synchronized List<AlertFact> alertFacts() {
        return new ArrayList<>(alertFacts);
    }

    /**
     * 可复盘的最早事件时间边界：当前仍保留事件的最小事件时间。
     * 一条都没有时返回 {@link Long#MAX_VALUE}（尚无数据）。
     */
    public synchronized long earliestEventTime() {
        long min = Long.MAX_VALUE;
        for (JournaledEvent e : events) {
            min = Math.min(min, e.event().eventTimeMillis());
        }
        return min;
    }

    /** 仍保留事件的最早线性化序列号；无事件返回 -1。 */
    public synchronized long earliestEventSeq() {
        JournaledEvent first = events.peekFirst();
        return first == null ? -1L : first.seq();
    }

    /** 仍保留告警事实的最早序列号；无事实返回 -1。 */
    public synchronized long earliestFactSeq() {
        AlertFact first = alertFacts.peekFirst();
        return first == null ? -1L : first.seq();
    }

    /** 最大已分配线性化序列号。 */
    public synchronized long lastSeq() {
        return seqCounter;
    }

    public synchronized int eventCount() {
        return events.size();
    }

    public synchronized int ruleChangeCount() {
        return ruleChanges.size();
    }

    public synchronized int alertFactCount() {
        return alertFacts.size();
    }

    /** 因容量而丢弃的事件数（可观测）。 */
    public synchronized long droppedEvents() {
        return droppedEvents;
    }

    /** 因容量而丢弃的告警事实数（可观测）。 */
    public synchronized long droppedAlertFacts() {
        return droppedFacts;
    }

    /** 保存（覆盖）日志老化时刻的告警状态检查点。 */
    public synchronized void saveCheckpoint(AlertCheckpoint cp) {
        this.checkpoint = cp;
    }

    /** 读取最近一次老化检查点；从未老化返回 null。 */
    public synchronized AlertCheckpoint checkpoint() {
        return checkpoint;
    }

    public int maxEvents() {
        return maxEvents;
    }
}

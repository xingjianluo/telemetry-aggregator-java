package com.github.xingjianluo.telemetryaggregatorjava.core;

import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事件幂等去重表。
 *
 * <p>以 {@code eventId} 为幂等键。负载指纹（SHA-256 over 规范化 JSON 语义内容）
 * 相同 = 重复投递（{@code DUPLICATE}，永不重复计数，且重试拿到与首次完全一致的
 * 结论）；eventId 相同但指纹不同 = 标识互相冲突（{@code ID_CONFLICT}）。</p>
 *
 * <p>条目内持有首次投递的 {@link CompletableFuture}：在首次事件仍在队列中处理时
 * 到达的重复投递直接共享同一个 future，结论天然一致。</p>
 *
 * <p>{@link #classify} 与 {@link #rollbackNew} 假定由聚合引擎在按 eventId 分条纹的
 * 锁下调用，保证同 id 的首次投递与重试互斥。</p>
 */
public final class DedupTable {

    /** 一次 classify 的判定。 */
    public enum Decision { NEW, DUPLICATE, CONFLICT, CAPACITY }

    /** classify 的结果。 */
    public static final class Classification {
        private final Decision decision;
        private final Entry entry;

        public Classification(Decision decision, Entry entry) {
            this.decision = decision;
            this.entry = entry;
        }

        public Decision getDecision() {
            return decision;
        }

        public Entry getEntry() {
            return entry;
        }
    }

    /** 去重条目。 */
    public static final class Entry {
        private final String fingerprint;
        private final CompletableFuture<IngestResult> future;
        private volatile boolean completed;

        public Entry(String fingerprint, CompletableFuture<IngestResult> future) {
            this.fingerprint = fingerprint;
            this.future = future;
        }

        public String getFingerprint() {
            return fingerprint;
        }

        public CompletableFuture<IngestResult> future() {
            return future;
        }

        public boolean isCompleted() {
            return completed;
        }

        public void markCompleted() {
            this.completed = true;
        }
    }

    private final int capacity;
    private final ConcurrentHashMap<String, Entry> table = new ConcurrentHashMap<>();

    public DedupTable(int capacity) {
        this.capacity = capacity;
    }

    /**
     * 计算事件负载指纹：按 key 排序后的规范内容哈希，使字段顺序不同不影响判重，
     * 而任一字段值不同都判为冲突。eventId 不参与（它是键）。
     */
    public static String fingerprint(TelemetryEvent event) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("t=").append(event.getTimestampMillis());
        sb.append("|m=").append(nullSafe(event.getMetric()));
        sb.append("|v=").append(event.getValue());
        sb.append("|tags=").append(canonicalTags(event.getTags()));
        sb.append("|tr=").append(nullSafe(event.getTraceId()));
        sb.append("|sp=").append(nullSafe(event.getSpanId()));
        sb.append("|pp=").append(nullSafe(event.getParentSpanId()));
        sb.append("|du=").append(event.getDurationMillis());
        return sha256(sb.toString());
    }

    private static String canonicalTags(Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return "{}";
        }
        TreeMap<String, String> sorted = new TreeMap<>(tags);
        StringBuilder sb = new StringBuilder("{");
        sorted.forEach((k, v) -> sb.append(nullSafe(k)).append('=')
                .append(nullSafe(v)).append(','));
        sb.setCharAt(sb.length() - 1, '}');
        return sb.toString();
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }

    private static String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /**
     * 判定一个 (eventId, fingerprint)。
     *
     * @param pendingFuture 首次投递时用于承载最终结论的 future（由调用方构造）
     */
    public Classification classify(String eventId, String fingerprint,
                                   CompletableFuture<IngestResult> pendingFuture) {
        Entry newEntry = new Entry(fingerprint, pendingFuture);
        // putIfAbsent 保证同 id 的并发首投只有一个胜出
        Entry existing = table.putIfAbsent(eventId, newEntry);
        if (existing == null) {
            if (table.size() > capacity) {
                // 超容量：移除本次占位并拒绝，不影响已有条目
                table.remove(eventId, newEntry);
                return new Classification(Decision.CAPACITY, null);
            }
            return new Classification(Decision.NEW, newEntry);
        }
        if (existing.fingerprint.equals(fingerprint)) {
            return new Classification(Decision.DUPLICATE, existing);
        }
        return new Classification(Decision.CONFLICT, existing);
    }

    /** 首次投递在进入队列前被拒绝（如 QUEUE_FULL）时回滚其 NEW 占用。 */
    public void rollbackNew(String eventId) {
        table.remove(eventId);
    }

    public int size() {
        return table.size();
    }
}

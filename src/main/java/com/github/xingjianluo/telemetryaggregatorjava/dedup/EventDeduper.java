package com.github.xingjianluo.telemetryaggregatorjava.dedup;

import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 有界的事件 ID 幂等登记表。
 *
 * <p>规则（确定且可复现）：
 * <ul>
 *   <li>首次见到的事件 ID：返回 {@link CheckResult#unknown()}，由调用方完成处理后
 *       通过 {@link #commit} 登记首次结论；</li>
 *   <li>相同 ID + 相同载荷：永远返回首次结论（重复投递、任意重试都不会二次计数）；</li>
 *   <li>相同 ID + 不同载荷：标记为 {@link RejectReason#CONFLICTING_EVENT_ID}；</li>
 *   <li>登记表达到上限时，新 ID 被拒绝（{@link RejectReason#TOO_MANY_EVENT_IDS}），
 *       不做静默驱逐，从而杜绝“驱逐后重试被二次计数”；</li>
 *   <li>水位线推进后，不可能再以非迟到身份出现的旧条目被显式回收。</li>
 * </ul>
 */
public class EventDeduper {

    /** 幂等检查结论。 */
    public record CheckResult(IngestResult verdict, boolean conflicting) {
        static CheckResult unknown() {
            return new CheckResult(null, false);
        }
    }

    private record Entry(String payloadHash, long eventTimeMillis, IngestResult verdict) {
    }

    /** 不驱逐、只按水位线老化，保证任何登记过的 ID 都不会被当成新事件。 */
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>();
    private final int maxEventIds;

    public EventDeduper(int maxEventIds) {
        this.maxEventIds = maxEventIds;
    }

    public synchronized CheckResult check(TelemetryEvent event) {
        Entry existing = entries.get(event.eventId());
        if (existing == null) {
            return CheckResult.unknown();
        }
        String hash = payloadHash(event);
        if (!hash.equals(existing.payloadHash)) {
            return new CheckResult(null, true);
        }
        return new CheckResult(existing.verdict, false);
    }

    /**
     * 登记首次结论。仅当表内尚无该 ID 时生效；已满则返回 false，由调用方拒绝新 ID。
     */
    public synchronized boolean commit(TelemetryEvent event, IngestResult verdict) {
        if (entries.containsKey(event.eventId())) {
            return true;
        }
        if (entries.size() >= maxEventIds) {
            return false;
        }
        entries.put(event.eventId(), new Entry(payloadHash(event), event.eventTimeMillis(), verdict));
        return true;
    }

    /**
     * 回收事件时间早于 horizon 的条目。调用方必须保证：这些 ID 即使以任何顺序重试，
     * 其所属窗口都已定稿，从而重试只会得到确定性迟到拒绝，不可能被二次计数。
     */
    public synchronized void prune(long horizonMillis) {
        Iterator<Map.Entry<String, Entry>> it = entries.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Entry> e = it.next();
            if (e.getValue().eventTimeMillis() < horizonMillis) {
                it.remove();
            }
        }
    }

    public synchronized int size() {
        return entries.size();
    }

    /** 载荷指纹：对标签先按 key 排序，保证等价载荷得到相同哈希。 */
    static String payloadHash(TelemetryEvent event) {
        StringBuilder sb = new StringBuilder(256);
        sb.append(event.eventTimeMillis()).append('|')
                .append(nullToEmpty(event.traceId())).append('|')
                .append(nullToEmpty(event.spanId())).append('|')
                .append(nullToEmpty(event.parentSpanId())).append('|')
                .append(nullToEmpty(event.service())).append('|')
                .append(nullToEmpty(event.metric())).append('|')
                .append(event.value()).append('|');
        Map<String, String> sorted = event.tags() == null
                ? Map.of() : new TreeMap<>(event.tags());
        List<String> parts = new ArrayList<>();
        sorted.forEach((k, v) -> parts.add(k + "=" + v));
        sb.append(String.join(",", parts));
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}

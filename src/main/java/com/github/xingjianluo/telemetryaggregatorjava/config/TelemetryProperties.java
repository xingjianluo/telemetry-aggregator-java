package com.github.xingjianluo.telemetryaggregatorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 遥测聚合器的全部可调参数，均带确定默认值，可通过 application.properties 覆盖。
 */
@ConfigurationProperties(prefix = "telemetry")
public class TelemetryProperties {

    private final Window window = new Window();
    private final Buffer buffer = new Buffer();
    private final Cardinality cardinality = new Cardinality();
    private final Trace trace = new Trace();
    private final Alert alert = new Alert();
    private final Rules rules = new Rules();
    private final Journal journal = new Journal();
    private final Replay replay = new Replay();

    public Window getWindow() { return window; }
    public Buffer getBuffer() { return buffer; }
    public Cardinality getCardinality() { return cardinality; }
    public Trace getTrace() { return trace; }
    public Alert getAlert() { return alert; }
    public Rules getRules() { return rules; }
    public Journal getJournal() { return journal; }
    public Replay getReplay() { return replay; }

    /** 窗口与水位线参数。 */
    public static class Window {
        /** 滚动窗口长度（毫秒）。 */
        private long sizeMillis = 10_000;
        /** 允许迟到：事件时间不得早于 watermark - allowedLateness。 */
        private long allowedLatenessMillis = 5_000;
        /** 无新事件时，水位线是否仍按墙上时钟继续向前推进。 */
        private boolean advanceOnIdle = true;
        /** 水位线空闲推进的检查间隔。 */
        private Duration tickInterval = Duration.ofMillis(100);

        public long getSizeMillis() { return sizeMillis; }
        public void setSizeMillis(long sizeMillis) { this.sizeMillis = sizeMillis; }
        public long getAllowedLatenessMillis() { return allowedLatenessMillis; }
        public void setAllowedLatenessMillis(long allowedLatenessMillis) { this.allowedLatenessMillis = allowedLatenessMillis; }
        public boolean isAdvanceOnIdle() { return advanceOnIdle; }
        public void setAdvanceOnIdle(boolean advanceOnIdle) { this.advanceOnIdle = advanceOnIdle; }
        public Duration getTickInterval() { return tickInterval; }
        public void setTickInterval(Duration tickInterval) { this.tickInterval = tickInterval; }
    }

    /** 摄取管道有界缓冲与背压参数。 */
    public static class Buffer {
        /** 待处理事件队列容量。 */
        private int queueCapacity = 10_000;
        /** 入队最长等待时间，超时按 BUFFER_FULL 拒绝。 */
        private Duration offerTimeout = Duration.ofMillis(50);
        /** 调用方等待最终处理结果的最长时间。 */
        private Duration resultTimeout = Duration.ofSeconds(2);

        public int getQueueCapacity() { return queueCapacity; }
        public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
        public Duration getOfferTimeout() { return offerTimeout; }
        public void setOfferTimeout(Duration offerTimeout) { this.offerTimeout = offerTimeout; }
        public Duration getResultTimeout() { return resultTimeout; }
        public void setResultTimeout(Duration resultTimeout) { this.resultTimeout = resultTimeout; }
    }

    /** 基数与内存有界保护参数。 */
    public static class Cardinality {
        /** 去重事件 ID 池上限（LRU）。 */
        private int maxEventIds = 200_000;
        /** 单窗口内活跃序列（标签指纹）数上限。 */
        private int maxSeriesPerWindow = 5_000;
        /** 保留的已关闭窗口结果数上限。 */
        private int maxSealedWindows = 1_000;
        /** 同时保留的调用链数量上限。 */
        private int maxTraces = 10_000;
        /** 单窗口内 trace 聚合序列数上限（&lt;=0 表示取 maxTraces）。 */
        private int maxTraceSeriesPerWindow = -1;
        /** 单条调用链内 span 数量上限。 */
        private int maxSpansPerTrace = 200;
        /** 单个事件允许的标签个数上限。 */
        private int maxTagsPerEvent = 20;
        /** 标签 key/value 字符串长度上限。 */
        private int maxTagValueLength = 200;

        public int getMaxEventIds() { return maxEventIds; }
        public void setMaxEventIds(int maxEventIds) { this.maxEventIds = maxEventIds; }
        public int getMaxSeriesPerWindow() { return maxSeriesPerWindow; }
        public void setMaxSeriesPerWindow(int maxSeriesPerWindow) { this.maxSeriesPerWindow = maxSeriesPerWindow; }
        public int getMaxSealedWindows() { return maxSealedWindows; }
        public void setMaxSealedWindows(int maxSealedWindows) { this.maxSealedWindows = maxSealedWindows; }
        public int getMaxTraces() { return maxTraces; }
        public void setMaxTraces(int maxTraces) { this.maxTraces = maxTraces; }
        public int getMaxTraceSeriesPerWindow() {
            return maxTraceSeriesPerWindow <= 0 ? maxTraces : maxTraceSeriesPerWindow;
        }
        public void setMaxTraceSeriesPerWindow(int maxTraceSeriesPerWindow) {
            this.maxTraceSeriesPerWindow = maxTraceSeriesPerWindow;
        }
        public int getMaxSpansPerTrace() { return maxSpansPerTrace; }
        public void setMaxSpansPerTrace(int maxSpansPerTrace) { this.maxSpansPerTrace = maxSpansPerTrace; }
        public int getMaxTagsPerEvent() { return maxTagsPerEvent; }
        public void setMaxTagsPerEvent(int maxTagsPerEvent) { this.maxTagsPerEvent = maxTagsPerEvent; }
        public int getMaxTagValueLength() { return maxTagValueLength; }
        public void setMaxTagValueLength(int maxTagValueLength) { this.maxTagValueLength = maxTagValueLength; }
    }

    /** 调用链还原参数。 */
    public static class Trace {
        /** 空闲调用链的保留时间，用于有界回收。 */
        private Duration retention = Duration.ofMinutes(10);

        public Duration getRetention() { return retention; }
        public void setRetention(Duration retention) { this.retention = retention; }
    }

    /** 规则版本化参数。 */
    public static class Rules {
        /** 保留的规则变更版本数上限；超出后最老版本折叠进基线，更早边界不可复盘。 */
        private int maxVersions = 1_000;
        /** 规则变更幂等键登记表上限；超出淘汰最老键（&lt;=0 表示取 maxVersions*2）。 */
        private int maxIdempotencyKeys = 2_000;

        public int getMaxVersions() { return maxVersions; }
        public void setMaxVersions(int maxVersions) { this.maxVersions = maxVersions; }
        public int getMaxIdempotencyKeys() { return maxIdempotencyKeys; }
        public void setMaxIdempotencyKeys(int maxIdempotencyKeys) { this.maxIdempotencyKeys = maxIdempotencyKeys; }
    }

    /** 复盘事件日志参数。 */
    public static class Journal {
        /** 日志保留的最大事件条数（含确定拒绝的结论），超出按事件时间最老淘汰。 */
        private int maxEvents = 100_000;

        public int getMaxEvents() { return maxEvents; }
        public void setMaxEvents(int maxEvents) { this.maxEvents = maxEvents; }
    }

    /** 复盘资源边界参数。 */
    public static class Replay {
        /** 单次复盘允许处理的最大事件条数，超出显式返回 TOO_LARGE。 */
        private int maxEvents = 200_000;
        /** 调用方等待复盘结果的最长时间；超时快速失败（BUSY），不拖住调用方。 */
        private Duration timeout = Duration.ofSeconds(30);

        public int getMaxEvents() { return maxEvents; }
        public void setMaxEvents(int maxEvents) { this.maxEvents = maxEvents; }
        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration timeout) { this.timeout = timeout; }
    }

    /** 告警稳定性参数。 */
    public static class Alert {
        /** 连续超阈值次数达到该值才进入 FIRING。 */
        private int breachStreak = 2;
        /** 连续恢复次数达到该值才回到 OK。 */
        private int recoverStreak = 2;
        /** FIRING 后再次允许状态翻转的最短间隔。 */
        private Duration cooldown = Duration.ofSeconds(30);
        /** 每个“规则 × 对象”保留的最近判定依据（AlertEvidence）条数上限。 */
        private int evidenceTrailSize = 64;

        public int getBreachStreak() { return breachStreak; }
        public void setBreachStreak(int breachStreak) { this.breachStreak = breachStreak; }
        public int getRecoverStreak() { return recoverStreak; }
        public void setRecoverStreak(int recoverStreak) { this.recoverStreak = recoverStreak; }
        public Duration getCooldown() { return cooldown; }
        public void setCooldown(Duration cooldown) { this.cooldown = cooldown; }
        public int getEvidenceTrailSize() { return evidenceTrailSize; }
        public void setEvidenceTrailSize(int evidenceTrailSize) { this.evidenceTrailSize = evidenceTrailSize; }
    }
}

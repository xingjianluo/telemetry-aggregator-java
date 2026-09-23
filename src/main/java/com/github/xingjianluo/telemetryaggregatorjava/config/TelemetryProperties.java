package com.github.xingjianluo.telemetryaggregatorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 遥测聚合引擎配置，前缀 {@code telemetry}，与 application.properties 衔接。
 */
@ConfigurationProperties(prefix = "telemetry")
public class TelemetryProperties {

    /** 事件时间窗口长度（毫秒）。 */
    private long windowMillis = 1000;

    /**
     * 最大迟到容忍（毫秒）。水位线 = 观察到的最大事件时间 - allowedLatenessMillis。
     * 事件时间 &lt; 水位线的事件按 LATE_BEHIND_WATERMARK 拒绝。
     */
    private long allowedLatenessMillis = 500;

    /** 摄取有界队列容量，满时按 QUEUE_FULL 拒绝（背压不阻塞写入线程）。 */
    private int ingestQueueCapacity = 10_000;

    /** 幂等去重表容量（eventId 条目上限），超出按 DEDUP_CAPACITY 拒绝。 */
    private int dedupCapacity = 1_000_000;

    /** 单个 tag key 在单个窗口内允许的不同 value 数（标签基数保护）。 */
    private int maxTagValuesPerKey = 10_000;

    /** 单个窗口内允许的不同 series（metric+标签组合）数。 */
    private int maxSeriesPerWindow = 100_000;

    /** 同时保留的活动窗口数上限。 */
    private int maxActiveWindows = 1000;

    /** 调用链 span 总数上限（防基数爆炸），超出按 TAG_CARDINALITY 类原因拒绝。 */
    private int maxSpans = 500_000;

    /** 已关闭窗口结果保留条数（供对外查询）。 */
    private int retainedWindows = 500;

    /** 告警变迁事件保留条数。 */
    private int retainedAlertEvents = 1000;

    public long getWindowMillis() {
        return windowMillis;
    }

    public void setWindowMillis(long windowMillis) {
        this.windowMillis = windowMillis;
    }

    public long getAllowedLatenessMillis() {
        return allowedLatenessMillis;
    }

    public void setAllowedLatenessMillis(long allowedLatenessMillis) {
        this.allowedLatenessMillis = allowedLatenessMillis;
    }

    public int getIngestQueueCapacity() {
        return ingestQueueCapacity;
    }

    public void setIngestQueueCapacity(int ingestQueueCapacity) {
        this.ingestQueueCapacity = ingestQueueCapacity;
    }

    public int getDedupCapacity() {
        return dedupCapacity;
    }

    public void setDedupCapacity(int dedupCapacity) {
        this.dedupCapacity = dedupCapacity;
    }

    public int getMaxTagValuesPerKey() {
        return maxTagValuesPerKey;
    }

    public void setMaxTagValuesPerKey(int maxTagValuesPerKey) {
        this.maxTagValuesPerKey = maxTagValuesPerKey;
    }

    public int getMaxSeriesPerWindow() {
        return maxSeriesPerWindow;
    }

    public void setMaxSeriesPerWindow(int maxSeriesPerWindow) {
        this.maxSeriesPerWindow = maxSeriesPerWindow;
    }

    public int getMaxActiveWindows() {
        return maxActiveWindows;
    }

    public void setMaxActiveWindows(int maxActiveWindows) {
        this.maxActiveWindows = maxActiveWindows;
    }

    public int getMaxSpans() {
        return maxSpans;
    }

    public void setMaxSpans(int maxSpans) {
        this.maxSpans = maxSpans;
    }

    public int getRetainedWindows() {
        return retainedWindows;
    }

    public void setRetainedWindows(int retainedWindows) {
        this.retainedWindows = retainedWindows;
    }

    public int getRetainedAlertEvents() {
        return retainedAlertEvents;
    }

    public void setRetainedAlertEvents(int retainedAlertEvents) {
        this.retainedAlertEvents = retainedAlertEvents;
    }
}

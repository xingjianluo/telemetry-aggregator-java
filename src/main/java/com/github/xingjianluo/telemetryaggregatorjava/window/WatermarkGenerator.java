package com.github.xingjianluo.telemetryaggregatorjava.window;

import java.time.Clock;

/**
 * 事件时间水位线生成器。
 *
 * <p>规则（确定、可复现）：
 * <ul>
 *   <li>窗口为固定滚动窗口：{@code windowStart = floor(eventTime / size) * size}，
 *       窗口归属只取决于事件时间；</li>
 *   <li>观察到事件时间 t 后：{@code watermark = max(watermark, maxEventTime - allowedLateness)}，
 *       即水位线表示“事件时间小于等于该值的数据理论上都到了”；</li>
 *   <li>{@code advanceOnIdle=true} 时，即使没有新事件，tick() 也会把水位线推进到
 *       {@code wallClock - allowedLateness}，保证无流量时窗口最终也会关闭；
 *       关闭该选项后水位线只由事件推进，便于在测试中精确复现；</li>
 *   <li>水位线单调不减；</li>
 *   <li>窗口 {@code [start, end)} 在 {@code watermark >= end} 时定稿关闭；
 *       事件时间 {@code t < watermark - allowedLateness}（其窗口已关闭且超过容忍）即迟到拒绝。</li>
 * </ul>
 */
public class WatermarkGenerator {

    private static final long NONE = Long.MIN_VALUE;

    /** 尚未观察到任何事件时间时的水位线哨兵值。 */
    public static long none() {
        return NONE;
    }

    private final long windowSizeMillis;
    private final long allowedLatenessMillis;
    private final boolean advanceOnIdle;
    private final Clock clock;

    private long maxEventTime = NONE;
    private long watermark = NONE;

    public WatermarkGenerator(long windowSizeMillis, long allowedLatenessMillis,
                              boolean advanceOnIdle, Clock clock) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis must be positive");
        }
        if (allowedLatenessMillis < 0) {
            throw new IllegalArgumentException("allowedLatenessMillis must be non-negative");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.allowedLatenessMillis = allowedLatenessMillis;
        this.advanceOnIdle = advanceOnIdle;
        this.clock = clock;
    }

    /** 用事件时间推进水位线，返回推进后的水位线。 */
    public synchronized long observe(long eventTimeMillis) {
        if (eventTimeMillis > maxEventTime) {
            maxEventTime = eventTimeMillis;
        }
        if (maxEventTime != NONE) {
            watermark = Math.max(watermark, maxEventTime - allowedLatenessMillis);
        }
        return watermark;
    }

    /** 空闲推进；在有界调度器中周期调用，返回推进后的水位线。 */
    public synchronized long tick() {
        if (advanceOnIdle) {
            watermark = Math.max(watermark, clock.millis() - allowedLatenessMillis);
        }
        return watermark;
    }

    public synchronized long currentWatermark() {
        return watermark;
    }

    /** 事件时间对应的滚动窗口起点。 */
    public static long windowStart(long eventTimeMillis, long windowSizeMillis) {
        return Math.floorDiv(eventTimeMillis, windowSizeMillis) * windowSizeMillis;
    }

    public static long windowEnd(long windowStartMillis, long windowSizeMillis) {
        return windowStartMillis + windowSizeMillis;
    }

    /** 窗口是否已到可关闭时刻。 */
    public static boolean sealable(long windowStartMillis, long windowSizeMillis, long watermarkMillis) {
        return watermarkMillis != NONE && windowEnd(windowStartMillis, windowSizeMillis) <= watermarkMillis;
    }

    /**
     * 事件是否超出迟到容忍：事件时间落后水位线超过 allowedLateness
     * （t &lt; watermark - allowedLateness）。这是最硬的拒绝边界。
     */
    public static boolean behindLatenessBound(long eventTimeMillis, long watermarkMillis,
                                              long allowedLatenessMillis) {
        return watermarkMillis != NONE && eventTimeMillis < watermarkMillis - allowedLatenessMillis;
    }

    /**
     * 事件所属窗口是否已经定稿关闭。关闭后的窗口不再接受任何事件
     * （在容忍期内到达、但窗口已关闭的事件同样明确拒绝，不静默计入）。
     */
    public static boolean windowAlreadySealed(long eventTimeMillis, long windowSizeMillis,
                                              long watermarkMillis) {
        if (watermarkMillis == NONE) {
            return false;
        }
        long start = windowStart(eventTimeMillis, windowSizeMillis);
        return sealable(start, windowSizeMillis, watermarkMillis);
    }
}

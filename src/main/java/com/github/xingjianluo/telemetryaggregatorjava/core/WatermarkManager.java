package com.github.xingjianluo.telemetryaggregatorjava.core;

/**
 * 事件时间水位线推进器。
 *
 * <p>确定且可复现的规则：</p>
 * <ul>
 *   <li>维护观察到的最大事件时间 {@code maxEventTime}（单调不减）。</li>
 *   <li>水位线 {@code W = maxEventTime - allowedLatenessMillis}，同样单调不减。</li>
 *   <li>迟到判定：{@code eventTime < W}（水位线表示“小于 W 的时间点都已到齐”，
 *       事件时间等于水位线仍被接受）。</li>
 *   <li>窗口关闭：end &lt;= W 的滚动窗口被 finalize（见 {@link WindowTable}）。</li>
 * </ul>
 *
 * <p>本类由聚合引擎单线程访问，无需加锁。</p>
 */
public final class WatermarkManager {

    private final long allowedLatenessMillis;
    // 由工作线程写、HTTP 摄取线程读，用 volatile 保证跨线程可见性
    private volatile long maxEventTime = Long.MIN_VALUE;
    private volatile long watermark = Long.MIN_VALUE;

    public WatermarkManager(long allowedLatenessMillis) {
        if (allowedLatenessMillis < 0) {
            throw new IllegalArgumentException("allowedLatenessMillis must be >= 0");
        }
        this.allowedLatenessMillis = allowedLatenessMillis;
    }

    /**
     * 观察到新的事件时间。仅在更大时推进（单调），返回推进后的水位线。
     * 未观察到任何事件时水位线为 Long.MIN_VALUE（表示无下界，任何事件都不迟到）。
     */
    public long observe(long eventTimeMillis) {
        if (eventTimeMillis > maxEventTime) {
            maxEventTime = eventTimeMillis;
            long candidate = maxEventTime - allowedLatenessMillis;
            if (candidate > watermark) {
                watermark = candidate;
            }
        }
        return watermark;
    }

    /**
     * 直接把水位线前推到给定事件时间对应的位置（单调，不可后退）。
     * 用于本地演示/测试在事件流停止后关闭残余窗口。
     */
    public long advanceTo(long eventTimeMillis) {
        return observe(eventTimeMillis);
    }

    public long currentWatermark() {
        return watermark;
    }

    public long maxObservedEventTime() {
        return maxEventTime;
    }

    /**
     * 事件是否迟到。未初始化（尚无事件、无手动推进）时不拒绝任何事件。
     */
    public boolean isLate(long eventTimeMillis) {
        return watermark != Long.MIN_VALUE && eventTimeMillis < watermark;
    }
}

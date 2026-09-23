package com.github.xingjianluo.telemetryaggregatorjava.model;

import java.util.List;

/**
 * 一个已关闭（finalized）窗口的不可变聚合快照，是对外查询与可解释性的载体。
 */
public final class WindowResult {

    private final long windowStart;
    private final long windowEnd;
    private final long closedAtWatermark;
    private final List<SeriesAggregate> series;

    public WindowResult(long windowStart, long windowEnd, long closedAtWatermark,
                        List<SeriesAggregate> series) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.closedAtWatermark = closedAtWatermark;
        this.series = List.copyOf(series);
    }

    public long getWindowStart() {
        return windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public long getClosedAtWatermark() {
        return closedAtWatermark;
    }

    public List<SeriesAggregate> getSeries() {
        return series;
    }
}

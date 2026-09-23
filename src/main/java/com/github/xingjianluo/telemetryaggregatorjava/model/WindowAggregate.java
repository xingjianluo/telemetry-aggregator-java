package com.github.xingjianluo.telemetryaggregatorjava.model;

import java.util.Map;

/**
 * 一个滚动窗口、一组标签指纹对应的聚合结论。结论在窗口关闭时定稿，可对外查询。
 */
public record WindowAggregate(
        long windowStartMillis,
        long windowEndMillis,
        String service,
        String metric,
        Map<String, String> tags,
        long count,
        double sum,
        double min,
        double max,
        /** 计入该聚合的不同事件 ID 数；幂等保证等于 count。 */
        long distinctEventCount
) {
    public double avg() {
        return count == 0 ? 0.0 : sum / count;
    }
}

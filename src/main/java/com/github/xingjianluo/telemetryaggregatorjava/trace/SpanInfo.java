package com.github.xingjianluo.telemetryaggregatorjava.trace;

/** 调用链中单个 span 的最小信息（用于查询与判定依据展示）。 */
public record SpanInfo(
        String spanId,
        String parentSpanId,
        long eventTimeMillis,
        double value
) {
}

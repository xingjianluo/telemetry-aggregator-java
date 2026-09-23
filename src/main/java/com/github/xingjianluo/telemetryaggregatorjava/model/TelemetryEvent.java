package com.github.xingjianluo.telemetryaggregatorjava.model;

import java.util.Map;

/**
 * 一条遥测事件（span 风格）。eventTimeMillis 为事件时间，是窗口归属的唯一依据；
 * traceId/spanId/parentSpanId 用于还原调用链。
 */
public record TelemetryEvent(
        String eventId,
        long eventTimeMillis,
        String traceId,
        String spanId,
        String parentSpanId,
        String service,
        String metric,
        double value,
        Map<String, String> tags
) {
}

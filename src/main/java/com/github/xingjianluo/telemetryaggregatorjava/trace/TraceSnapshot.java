package com.github.xingjianluo.telemetryaggregatorjava.trace;

import java.util.List;

/** 某条调用链当前的还原快照：状态、涉及 span、以及得出该状态的依据说明。 */
public record TraceSnapshot(
        String traceId,
        TraceStatus status,
        List<SpanInfo> spans,
        List<String> reasons,
        long firstEventTimeMillis,
        long lastEventTimeMillis
) {
}

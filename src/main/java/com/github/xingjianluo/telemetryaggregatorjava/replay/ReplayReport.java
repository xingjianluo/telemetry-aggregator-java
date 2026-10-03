package com.github.xingjianluo.telemetryaggregatorjava.replay;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceSnapshot;

import java.util.List;
import java.util.Map;

/**
 * 复盘报告：给定事件时间边界后，该边界内已定稿的窗口聚合、生效规则版本、
 * 告警状态与调用链结论，以及得出这些结论的依据。
 */
public record ReplayReport(
        ReplayStatus status,
        /** 状态说明（不可复盘时的明确原因）。 */
        String detail,
        long asOfMillis,
        /** 复盘水位线：只有 end <= 该值的窗口被认定已定稿。 */
        long replayWatermarkMillis,
        List<WindowAggregate> aggregates,
        List<AlertSnapshot> alerts,
        List<TraceSnapshot> traces,
        /** 边界内实际参照的规则版本历史。 */
        List<RuleChange> ruleChanges,
        /** 边界内被确定拒绝的事件按原因计数（审计依据）。 */
        Map<RejectReason, Long> rejectedByReason,
        long acceptedCount,
        long sealedWindowCount
) {
}

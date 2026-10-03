package com.github.xingjianluo.telemetryaggregatorjava.replay;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;

import java.util.List;

/**
 * 复盘时刻（as of T）某规则 × 评估对象的告警状态与依据。
 * 依据列表逐条给出“哪个窗口、哪个规则版本、值是多少、如何翻转/被抑制”。
 */
public record ReplayAlert(
        String ruleId,
        long ruleVersion,
        String scope,
        String subjectKey,
        AlertState state,
        int consecutiveBreaches,
        int consecutiveRecoveries,
        double lastValue,
        long lastEvaluatedWindowStart,
        String traceMarker,
        boolean quietAtCutoff,
        long suppressedCount,
        List<String> basis
) {
}

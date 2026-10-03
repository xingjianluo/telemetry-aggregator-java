package com.github.xingjianluo.telemetryaggregatorjava.alert;

import java.util.List;
import java.util.Map;

/**
 * 单个规则 + 单个评估对象（标签指纹或 traceId）的当前告警状态与判定依据。
 *
 * @param ruleVersion     当前用于判定的规则版本
 * @param quietActive     当前事件时间是否处于该规则的免打扰区间
 * @param suppressedCount 免打扰期间被抑制（证据照常累计、未对外翻转）的窗口数
 * @param basis           最近若干次评估的人类可读依据（可解释）
 */
public record AlertSnapshot(
        String ruleId,
        AlertRule.Scope scope,
        String subjectKey,
        Map<String, String> tags,
        AlertState state,
        int consecutiveBreaches,
        int consecutiveRecoveries,
        double lastValue,
        long lastEvaluatedWindowStart,
        long lastChangedMillis,
        String traceMarker,
        long ruleVersion,
        boolean quietActive,
        long suppressedCount,
        List<String> basis
) {
}

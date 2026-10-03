package com.github.xingjianluo.telemetryaggregatorjava.alert;

import java.util.List;
import java.util.Map;

/**
 * 一次“规则 × 评估对象 × 定稿窗口”的告警判定事实（不可变）。
 *
 * <p>实时判定每发生一次评估就追加一条；复盘时按事实序列折叠，
 * 因此每个告警结论都能说明：当时值是多少、用的哪个规则版本、
 * 是否越界、旧状态/新状态、是否被免打扰抑制、依据是什么。
 *
 * @param suppressed 免打扰抑制：证据照常累计，但本次不做对外状态翻转
 */
public record AlertFact(
        long seq,
        long windowStartMillis,
        long windowEndMillis,
        String ruleId,
        long ruleVersion,
        AlertRule.Scope scope,
        String subjectKey,
        Map<String, String> tags,
        double value,
        boolean breach,
        AlertState fromState,
        AlertState toState,
        boolean suppressed,
        String traceMarker,
        List<String> reasons
) {
}

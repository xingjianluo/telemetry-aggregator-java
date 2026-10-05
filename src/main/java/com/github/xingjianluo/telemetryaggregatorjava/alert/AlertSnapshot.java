package com.github.xingjianluo.telemetryaggregatorjava.alert;

import java.util.Map;

/**
 * 单个规则 × 单个评估对象（标签指纹或 traceId）的当前告警状态与判定依据。
 * ruleVersion 标明最近一次判定使用的规则版本；muted 标明该对象当前处于免打扰区间。
 *
 * <p>{@code decisionBasis} 是可读的判定依据：依据哪个窗口的哪份证据（窗口区间、
 * 窗口均值）、命中或未命中哪个阈值、streak 累计 进度、状态是否翻转及未翻转原因
 * （确认次数不足 / 冷却期 / 免打扰抑制）。只由事件时间与规则版本决定，
 * 实时与复盘对同一条结论给出逐位一致的文本。
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
        /** 最近一次状态翻转发生的窗口 end（事件时间）。 */
        long lastChangedMillis,
        /** 调用链作用域下链路拓扑异常时的标记；TAGS 作用域为空。 */
        String traceMarker,
        /** 最近一次判定使用的规则版本（0 为初始规则集）。 */
        long ruleVersion,
        /** 最近一次判定时该对象是否处于免打扰区间。 */
        boolean muted,
        /** 最近一次判定的可读依据（确定性文本，复盘逐位复现）。 */
        String decisionBasis
) {
}

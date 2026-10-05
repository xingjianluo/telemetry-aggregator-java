package com.github.xingjianluo.telemetryaggregatorjava.alert;

import java.util.List;
import java.util.Map;

/**
 * 单个规则 × 单个评估对象（标签指纹或 traceId）的当前告警状态与判定依据。
 * ruleVersion 标明最近一次判定使用的规则版本；muted 标明该对象当前处于免打扰区间。
 *
 * @param evidence 判定链：按事件时间升序的最近若干次窗口评估依据
 *                 （用的哪个窗口、哪份聚合、哪个规则版本/阈值、为什么翻转或没翻转、
 *                 是否免打扰、抑制期间是否累计）。实时与复盘在同一边界下逐位一致。
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
        /** 可读判定依据链（事件时间升序，有界）。 */
        List<AlertEvidence> evidence
) {
}

package com.github.xingjianluo.telemetryaggregatorjava.rule;

import java.util.Map;

/**
 * 一次在线规则变更请求（不重启、不断流）。
 *
 * <p>语义：
 * <ul>
 *   <li>{@code id} 指定要调整的规则；不存在则新建（首个版本从边界开始生效）；</li>
 *   <li>阈值 / scope / service / metric / tagSelector 为匹配与判定参数；
 *       传 null 的字段默认沿用当前最新版本的值，新建时按内置默认补齐；</li>
 *   <li>{@code enabled} 控制启停（null 表示沿用）；</li>
 *   <li>{@code quietFromMillis/quietUntilMillis} 给出一段事件时间免打扰区间，
 *       两者同时为 null 表示取消免打扰；只传一个时另一个按默认处理；</li>
 *   <li>请求可携带 {@code requestId}：相同 requestId 的重试返回首次结论，不产生新版本。</li>
 * </ul>
 */
public record RuleChangeRequest(
        String requestId,
        String id,
        Boolean enabled,
        AlertRuleScope scope,
        String service,
        String metric,
        Map<String, String> tagSelector,
        Double threshold,
        AlertRuleOperator operator,
        Long quietFromMillis,
        Long quietUntilMillis,
        String note
) {
    /** 与 {@code AlertRule.Scope} 解耦的传输枚举（避免 Web 层直接依赖内部判定类型）。 */
    public enum AlertRuleScope { TAGS, TRACE }

    /** 传输用操作符枚举。 */
    public enum AlertRuleOperator { GT, GTE, LT, LTE }
}

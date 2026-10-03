package com.github.xingjianluo.telemetryaggregatorjava.alert;

/**
 * 规则变更请求（在线提交，无需重启）。
 *
 * <p>字段按动作要求：UPSERT 需要完整 {@code rule}；MUTE 需要 {@code muteUntilMillis}
 * （事件时间边界）；DISABLE/ENABLE/UNMUTE 只需要 {@code ruleId}。
 */
public record RuleChangeRequest(
        String ruleId,
        RuleChange.Action action,
        AlertRule rule,
        Long muteUntilMillis
) {
}

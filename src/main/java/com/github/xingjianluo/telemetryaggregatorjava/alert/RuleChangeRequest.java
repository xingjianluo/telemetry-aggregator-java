package com.github.xingjianluo.telemetryaggregatorjava.alert;

/**
 * 规则变更请求（在线提交，无需重启）。
 *
 * <p>字段按动作要求：UPSERT 需要完整 {@code rule}；MUTE 需要 {@code muteUntilMillis}
 * （事件时间边界）；DISABLE/ENABLE/UNMUTE 只需要 {@code ruleId}。
 *
 * <p>幂等键：{@code requestId} 由调用方生成，用于安全重试。同一个 {@code requestId}
 * 不管提交多少次，服务端只落一条版本，且每次返回同一条变更记录（同一版本号、
 * 同一生效边界）；相同 {@code requestId} 搭配不同载荷会被拒绝（冲突）。
 * 不携带 {@code requestId} 时每次提交都视为一次新变更。
 */
public record RuleChangeRequest(
        String ruleId,
        RuleChange.Action action,
        AlertRule rule,
        Long muteUntilMillis,
        /** 幂等键（可选）：网络超时 / 抖动重试时必须携带同一个值。 */
        String requestId
) {
    /** 兼容构造：不带幂等键的变更请求。 */
    public RuleChangeRequest(String ruleId, RuleChange.Action action, AlertRule rule,
                             Long muteUntilMillis) {
        this(ruleId, action, rule, muteUntilMillis, null);
    }

    /** 判定两次提交的载荷是否一致（幂等键相同时用于冲突检测）。 */
    public boolean samePayloadAs(RuleChangeRequest other) {
        return other != null
                && java.util.Objects.equals(ruleId, other.ruleId)
                && action == other.action
                && java.util.Objects.equals(rule, other.rule)
                && java.util.Objects.equals(muteUntilMillis, other.muteUntilMillis);
    }
}

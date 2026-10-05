package com.github.xingjianluo.telemetryaggregatorjava.alert;

/**
 * 规则变更请求（在线提交，无需重启）。
 *
 * <p>字段按动作要求：UPSERT 需要完整 {@code rule}；MUTE 需要 {@code muteUntilMillis}
 * （事件时间边界）；DISABLE/ENABLE/UNMUTE 只需要 {@code ruleId}。
 *
 * <p>幂等键 {@code requestId}：由调用方为“同一次逻辑变更”生成并在重试时保持不变
 * （推荐 UUID；HTTP 层也可用 Idempotency-Key 头）。服务端保证该键只落一个版本：
 * 键相同且载荷相同的重复 / 并发提交永远返回同一个版本号与同一个生效边界，
 * 不会第二次改动规则状态；键相同但载荷不同按非法变更拒绝（422），不产生版本。
 * 为 null 表示该次提交不登记幂等（不推荐，无法安全重试）。
 */
public record RuleChangeRequest(
        /** 幂等键：同一次逻辑变更的任意次重试必须相同；不同变更必须不同。 */
        String requestId,
        String ruleId,
        RuleChange.Action action,
        AlertRule rule,
        Long muteUntilMillis
) {
}

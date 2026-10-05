package com.github.xingjianluo.telemetryaggregatorjava.alert;

/**
 * 一次规则变更的不可变记录（版本化、可查询）。
 *
 * <p>生效边界语义：{@code effectiveFromMillis} 是变更被应用时刻的事件时间水位线。
 * 窗口结束时间 {@code end <= effectiveFromMillis} 的窗口在此之前已定稿，维持原判；
 * {@code end > effectiveFromMillis} 的窗口按变更后的规则判定。
 */
public record RuleChange(
        /** 单调递增的版本号，从 1 开始（0 为初始规则集）。 */
        long version,
        /** 产生该版本的幂等键；重试同一 requestId 永远返回这同一条记录。 */
        String requestId,
        String ruleId,
        Action action,
        /** UPSERT 后的规则快照；其余动作为变更前已知的规则快照（可能为 null）。 */
        AlertRule ruleSnapshot,
        /** MUTE 动作的免打扰截止（事件时间，窗口 end <= 该值时抑制判定）；否则为 null。 */
        Long muteUntilMillis,
        /** 生效边界（事件时间水位线）。 */
        long effectiveFromMillis,
        /** 墙上时钟应用时间（仅用于展示，不参与判定，保证复盘确定性）。 */
        long appliedAtMillis
) {
    /** 变更动作类型。 */
    public enum Action {
        /** 新建或更新规则定义（阈值/范围/操作符）。 */
        UPSERT,
        /** 停用规则：不再评估，但保留主体状态，重新启用后续接。 */
        DISABLE,
        /** 启用已停用的规则。 */
        ENABLE,
        /** 设置免打扰区间（事件时间 [-, muteUntil]）。 */
        MUTE,
        /** 提前解除免打扰。 */
        UNMUTE
    }
}

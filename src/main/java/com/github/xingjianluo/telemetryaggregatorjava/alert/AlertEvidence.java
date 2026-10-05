package com.github.xingjianluo.telemetryaggregatorjava.alert;

/**
 * 一次窗口定稿评估的完整、可读判定依据（审计链上的一环）。
 *
 * <p>所有字段只来源于<b>事件时间</b>（定稿窗口）与该窗口解析出的规则版本，
 * 不含墙上时钟、线程或随机信息，因此实时与复盘在同一边界下逐位一致，
 * 同一批输入重复评估永远得到同一依据。
 *
 * @param windowStartMillis 证据窗口起点（[start,end)，事件时间）
 * @param windowEndMillis   证据窗口结束（状态机的“现在”，也是生效边界判定基准）
 * @param avg               窗口聚合均值（比较对象）
 * @param count             窗口内样本数
 * @param ruleVersion       本窗口实际使用的规则版本（0=初始规则集）
 * @param threshold         本窗口阈值
 * @param operator          本窗口比较操作符
 * @param service           规则匹配的 service（null=全部）
 * @param metric            规则匹配的 metric（null=全部）
 * @param muted             本次评估是否处于免打扰区间
 * @param muteUntilMillis   当时的免打扰截止（0=未设置）
 * @param breach            均值是否越界
 * @param consecutiveBreaches 评估后的连续越界计数
 * @param consecutiveRecoveries 评估后的连续恢复计数
 * @param stateBefore       评估前状态
 * @param stateAfter        评估后状态
 * @param transition        本次是否发生状态翻转
 * @param suppressed        本次翻转是否被冷却抑制（越界/恢复已确认但冷却未满，维持原状态）
 * @param traceMarker       TRACE 作用域下的拓扑异常标记；TAGS 为 null
 * @param rationale         人类可读的判定说明（为什么翻 / 为什么没翻 / 是否免打扰）
 */
public record AlertEvidence(
        long windowStartMillis,
        long windowEndMillis,
        double avg,
        long count,
        long ruleVersion,
        double threshold,
        AlertRule.Operator operator,
        String service,
        String metric,
        boolean muted,
        long muteUntilMillis,
        boolean breach,
        int consecutiveBreaches,
        int consecutiveRecoveries,
        AlertState stateBefore,
        AlertState stateAfter,
        boolean transition,
        boolean suppressed,
        String traceMarker,
        String rationale
) {
}

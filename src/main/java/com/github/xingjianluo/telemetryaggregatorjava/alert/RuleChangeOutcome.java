package com.github.xingjianluo.telemetryaggregatorjava.alert;

/**
 * 规则变更提交的<b>成功</b>结论（失败以可区分的异常表达，见 IngestionEngine）。
 *
 * <p>同一个幂等键（{@link RuleChangeRequest#requestId()}）的任意次提交
 * （含超时后重试、并发重发）拿到的 {@link #change()} 都是同一条不可变记录：
 * 版本号、生效边界、规则快照逐位相同；{@link #replayed()} 标明本次是否为重放——
 * {@code false} 表示首次生效，{@code true} 表示该变更此前已生效、本次未产生任何副作用。
 */
public record RuleChangeOutcome(
        RuleChange change,
        /** false=首次提交并生效；true=幂等重放（此前已生效，本次无副作用）。 */
        boolean replayed
) {
    public static RuleChangeOutcome first(RuleChange change) {
        return new RuleChangeOutcome(change, false);
    }

    public static RuleChangeOutcome replay(RuleChange change) {
        return new RuleChangeOutcome(change, true);
    }
}

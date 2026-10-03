package com.github.xingjianluo.telemetryaggregatorjava.rule;

/**
 * 一次规则变更的结论：状态 + 新版本（或首次结论版本）+ 可解释 detail。
 * 重试（相同 requestId）永远得到同一结论，保证运营操作幂等。
 */
public record RuleChangeResult(
        String requestId,
        String ruleId,
        RuleChangeStatus status,
        Long version,
        /** 生效边界（事件时间毫秒）：该值之后定稿的窗口按此版本判定。 */
        Long effectiveFromMillis,
        String detail
) {
    public boolean applied() {
        return status == RuleChangeStatus.APPLIED || status == RuleChangeStatus.DUPLICATE_REQUEST;
    }

    public static RuleChangeResult invalid(RuleChangeRequest req, String detail) {
        String id = req == null ? "<missing>" : req.id();
        String rid = req == null ? null : req.requestId();
        return new RuleChangeResult(rid, id, RuleChangeStatus.INVALID_REQUEST, null, null, detail);
    }
}

package com.github.xingjianluo.telemetryaggregatorjava.replay;

import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion;

/**
 * 进入复盘日志的一条规则变更：与事件共用 {@code seq} 序列空间，
 * 并记录线性化时刻的水位线作为生效边界，保证复盘与实时判定同源。
 */
public record JournaledRuleChange(
        long seq,
        RuleVersion version,
        long watermarkAtApplyMillis
) {
}

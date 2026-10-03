package com.github.xingjianluo.telemetryaggregatorjava.replay;

import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceSnapshot;

import java.util.List;

/**
 * 按事件时间复盘的完整结论。只由输入事件与规则变更历史决定，
 * 纯只读重放，不会反过来改写任何实时状态。
 *
 * @param requestedAtMillis  请求复盘的时间点 T（事件时间）
 * @param watermarkMillis    复盘切点处的水位线
 * @param cutoffSeq          实际折叠到的线性化序列号
 * @param horizonMillis      当前可复盘的最早事件时间边界（早于此值不可复盘）
 * @param scannedEvents      本次复盘扫描的事件数（资源可观测）
 * @param capacityEvents     日志保留的事件容量
 */
public record ReplayReport(
        ReplayStatus status,
        String unavailableReason,
        long requestedAtMillis,
        long watermarkMillis,
        long cutoffSeq,
        long horizonMillis,
        long scannedEvents,
        long capacityEvents,
        List<WindowAggregate> aggregates,
        List<RuleVersion> rulesEffective,
        List<ReplayAlert> alerts,
        List<TraceSnapshot> traces,
        List<String> basis
) {
    public boolean replayable() {
        return status == ReplayStatus.COMPLETE || status == ReplayStatus.PARTIAL;
    }
}

package com.github.xingjianluo.telemetryaggregatorjava.rule;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;

import java.util.Map;

/**
 * 一条告警规则的某个不可变版本（规则运营的最小单位）。
 *
 * <p>每次在线变更（阈值 / 匹配范围 / 启停 / 免打扰区间）都产生一个新版本，
 * 版本单调递增；{@code effectiveFromMillis} 是该版本的生效边界（事件时间语义）：
 * 边界之前已经定稿的窗口维持原判，边界之后定稿的窗口按新版本判定。
 *
 * @param version               版本号，从 1 开始单调递增
 * @param id                    规则 ID（同一逻辑规则的不同版本共享）
 * @param enabled               是否启用；停用版本生效后，后续定稿窗口不再对该规则做评估
 * @param scope/service/metric/tagSelector/threshold/operator 匹配与判定参数
 * @param quietFromMillis       免打扰区间起点（事件时间）；&lt;0 表示无免打扰
 * @param quietUntilMillis      免打扰区间终点（事件时间，半开 [from, until)）；&lt;0 表示无
 * @param createdAtMillis       版本创建的墙上时间（仅审计展示用，不参与判定）
 * @param note                  变更说明
 */
public record RuleVersion(
        long version,
        String id,
        boolean enabled,
        AlertRule.Scope scope,
        String service,
        String metric,
        Map<String, String> tagSelector,
        double threshold,
        AlertRule.Operator operator,
        long quietFromMillis,
        long quietUntilMillis,
        /** 生效边界（事件时间）：effectiveFrom &lt; windowEnd 的最新版本用于该窗口判定。 */
        long effectiveFromMillis,
        long createdAtMillis,
        String note
) {
    /** 转化为判定用的规则定义。 */
    public AlertRule toAlertRule() {
        return new AlertRule(id, scope, service, metric,
                tagSelector == null ? Map.of() : Map.copyOf(tagSelector),
                threshold, operator);
    }

    /** 给定事件时间点是否处于免打扰区间内（半开区间 [from, until)）。 */
    public boolean quietAt(long eventTimeMillis) {
        return quietFromMillis >= 0 && eventTimeMillis >= quietFromMillis
                && (quietUntilMillis < 0 || eventTimeMillis < quietUntilMillis);
    }

    /** 免打扰区间是否覆盖给定事件时间窗口 [wStart, wEnd) 的任意部分。 */
    public boolean quietOverlapsWindow(long wStart, long wEnd) {
        if (quietFromMillis < 0) {
            return false;
        }
        long until = quietUntilMillis < 0 ? Long.MAX_VALUE : quietUntilMillis;
        return quietFromMillis < wEnd && until > wStart;
    }
}

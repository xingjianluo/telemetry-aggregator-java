package com.github.xingjianluo.telemetryaggregatorjava.alert;

import java.util.Map;

/**
 * 一条告警规则。scope=TAGS 时按标签序列评估；scope=TRACE 时按调用链评估。
 * 阈值比较针对窗口聚合的 avg。
 */
public record AlertRule(
        String id,
        Scope scope,
        String service,
        String metric,
        /** 仅对 TAGS 作用域生效：标签选择器，空 map 表示匹配全部。 */
        Map<String, String> tagSelector,
        double threshold,
        Operator operator
) {
    public enum Scope { TAGS, TRACE }

    public enum Operator { GT, GTE, LT, LTE }

    public boolean breaches(double value) {
        return switch (operator) {
            case GT -> value > threshold;
            case GTE -> value >= threshold;
            case LT -> value < threshold;
            case LTE -> value <= threshold;
        };
    }
}

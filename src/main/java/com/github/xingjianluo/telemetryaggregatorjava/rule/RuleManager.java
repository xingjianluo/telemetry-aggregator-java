package com.github.xingjianluo.telemetryaggregatorjava.rule;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 规则变更的校验与版本构建（无状态工具 + 幂等 requestId 表）。
 *
 * <p>本类不决定生效边界：边界由摄取 worker 在线性化时刻按当前水位线给出，
 * 保证“边界之前已定稿的窗口维持原判、之后按新规则判定”。
 * 相同 requestId 的任意重试都返回首次结论，不产生第二个版本。
 */
public class RuleManager {

    /** 已处理的 requestId -> 首次结论。 */
    private final Map<String, RuleChangeResult> idempotency = new LinkedHashMap<>();

    /** 查询 requestId 的首次结论（worker 单线程调用）；无则 null。 */
    public RuleChangeResult firstResult(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return null;
        }
        return idempotency.get(requestId);
    }

    /** 登记 requestId 的首次结论（worker 单线程调用）。 */
    public void remember(String requestId, RuleChangeResult result) {
        if (requestId != null && !requestId.isBlank()) {
            idempotency.put(requestId, result);
        }
    }

    /** 校验变更请求。返回非 null 表示非法。 */
    public String validate(RuleChangeRequest req) {
        if (req == null) {
            return "请求体为空";
        }
        if (req.id() == null || req.id().isBlank()) {
            return "缺少规则 id";
        }
        if (req.tagSelector() != null) {
            for (String k : req.tagSelector().keySet()) {
                if (k == null || k.isBlank()) {
                    return "标签选择器的 key 不能为空";
                }
            }
        }
        if (req.threshold() != null && (Double.isNaN(req.threshold()) || Double.isInfinite(req.threshold()))) {
            return "threshold 必须是有限数值";
        }
        Long qf = req.quietFromMillis();
        Long qu = req.quietUntilMillis();
        if (qf != null && qf < 0) {
            return "quietFromMillis 不能为负";
        }
        if (qu != null && qu < 0) {
            return "quietUntilMillis 不能为负";
        }
        if (qf != null && qu != null && qu < qf) {
            return "免打扰区间倒置：quietUntilMillis < quietFromMillis";
        }
        if (req.scope() != null) {
            try {
                com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Scope.valueOf(req.scope().name());
            } catch (IllegalArgumentException e) {
                return "scope 只能是 TAGS / TRACE";
            }
        }
        if (req.operator() != null) {
            try {
                com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Operator.valueOf(req.operator().name());
            } catch (IllegalArgumentException e) {
                return "operator 只能是 GT/GTE/LT/LTE";
            }
        }
        return null;
    }

    /** 合并后的版本参数（未提供的字段沿用最新版本；新建规则按内置默认）。 */
    public record MergedSpec(
            boolean enabled,
            com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Scope scope,
            String service,
            String metric,
            Map<String, String> tagSelector,
            double threshold,
            com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Operator operator,
            long quietFromMillis,
            long quietUntilMillis
    ) {
    }

    /**
     * 基于最新版本合并请求字段。
     *
     * @param latest 该规则当前最新版本；null 表示新建
     */
    public MergedSpec merge(RuleChangeRequest req, RuleVersion latest) {
        boolean enabled;
        com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Scope scope;
        String service;
        String metric;
        Map<String, String> tagSelector;
        double threshold;
        com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Operator operator;
        long quietFrom;
        long quietUntil;

        if (latest == null) {
            enabled = req.enabled() == null || req.enabled();
            scope = req.scope() == null
                    ? com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Scope.TAGS
                    : com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Scope.valueOf(req.scope().name());
            service = req.service();
            metric = req.metric();
            tagSelector = req.tagSelector() == null ? Map.of() : new LinkedHashMap<>(req.tagSelector());
            threshold = req.threshold() == null ? 100.0 : req.threshold();
            operator = req.operator() == null
                    ? com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Operator.GT
                    : com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Operator.valueOf(req.operator().name());
        } else {
            enabled = req.enabled() == null ? latest.enabled() : req.enabled();
            scope = req.scope() == null ? latest.scope()
                    : com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Scope.valueOf(req.scope().name());
            service = req.service() == null ? latest.service() : req.service();
            metric = req.metric() == null ? latest.metric() : req.metric();
            tagSelector = req.tagSelector() == null
                    ? new LinkedHashMap<>(latest.tagSelector())
                    : new LinkedHashMap<>(req.tagSelector());
            threshold = req.threshold() == null ? latest.threshold() : req.threshold();
            operator = req.operator() == null ? latest.operator()
                    : com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule.Operator.valueOf(req.operator().name());
        }

        // 免打扰：显式给负数无法到达（validate 已拒绝）；from/until 都为 null 表示沿用；
        // 想清除区间：请求里把 quietFromMillis 与 quietUntilMillis 显式置为同一值（0,0），
        // 由调用方在 Web 层提供语义糖，这里保持字段直传。
        if (req.quietFromMillis() == null && req.quietUntilMillis() == null) {
            quietFrom = latest == null ? -1L : latest.quietFromMillis();
            quietUntil = latest == null ? -1L : latest.quietUntilMillis();
        } else {
            quietFrom = req.quietFromMillis() == null
                    ? (latest == null ? -1L : latest.quietFromMillis()) : req.quietFromMillis();
            quietUntil = req.quietUntilMillis() == null
                    ? (latest == null ? -1L : latest.quietUntilMillis()) : req.quietUntilMillis();
        }
        return new MergedSpec(enabled, scope, emptyToNull(service), emptyToNull(metric),
                tagSelector, threshold, operator, quietFrom, quietUntil);
    }

    /** 组装一条人类可读的变更说明（审计展示）。 */
    public static String describeChange(RuleChangeRequest req, RuleVersion latest, MergedSpec spec) {
        List<String> parts = new ArrayList<>();
        if (latest == null) {
            parts.add("新建规则");
        } else {
            if (req.enabled() != null && req.enabled() != latest.enabled()) {
                parts.add(req.enabled() ? "启用" : "停用");
            }
            if (req.threshold() != null && req.threshold() != latest.threshold()) {
                parts.add("阈值 " + latest.threshold() + "->" + req.threshold());
            }
            if (req.operator() != null && !req.operator().name().equals(latest.operator().name())) {
                parts.add("操作符 " + latest.operator() + "->" + req.operator());
            }
            if (req.service() != null) {
                parts.add("service=" + req.service());
            }
            if (req.metric() != null) {
                parts.add("metric=" + req.metric());
            }
            if (req.tagSelector() != null) {
                parts.add("标签选择器=" + req.tagSelector());
            }
            if (req.scope() != null) {
                parts.add("scope=" + spec.scope());
            }
            if (req.quietFromMillis() != null || req.quietUntilMillis() != null) {
                parts.add("免打扰区间[" + spec.quietFromMillis() + "," + spec.quietUntilMillis() + ")");
            }
        }
        if (parts.isEmpty()) {
            parts.add("无实质字段变化（产生空版本）");
        }
        if (req.note() != null && !req.note().isBlank()) {
            parts.add("note=" + req.note());
        }
        return String.join("；", parts);
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }
}

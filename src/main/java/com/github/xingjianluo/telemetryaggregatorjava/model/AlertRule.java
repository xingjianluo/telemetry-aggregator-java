package com.github.xingjianluo.telemetryaggregatorjava.model;

/**
 * 告警规则。判定对象为还原后的调用链：{@code metric=CHAIN_DURATION} 时
 * 取链路总耗时，{@code metric=CHAIN_DEPTH} 时取链路深度。
 *
 * <p>稳定性（防抖动）规则：值连续 {@code fireConsecutive} 次评估超过
 * {@code fireThreshold} 才进入 FIRING；FIRING 后连续
 * {@code resolveConsecutive} 次评估低于 {@code resolveThreshold} 才恢复 OK。
 * 两个阈值可以不同（滞后带 hysteresis band）。</p>
 */
public final class AlertRule {

    private final String ruleId;
    private final String traceId;
    private final String metric;
    private final double fireThreshold;
    private final double resolveThreshold;
    private final int fireConsecutive;
    private final int resolveConsecutive;

    public AlertRule(String ruleId, String traceId, String metric,
                     double fireThreshold, double resolveThreshold,
                     int fireConsecutive, int resolveConsecutive) {
        this.ruleId = ruleId;
        this.traceId = traceId;
        this.metric = metric;
        this.fireThreshold = fireThreshold;
        this.resolveThreshold = resolveThreshold;
        this.fireConsecutive = fireConsecutive;
        this.resolveConsecutive = resolveConsecutive;
    }

    public String getRuleId() {
        return ruleId;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getMetric() {
        return metric;
    }

    public double getFireThreshold() {
        return fireThreshold;
    }

    public double getResolveThreshold() {
        return resolveThreshold;
    }

    public int getFireConsecutive() {
        return fireConsecutive;
    }

    public int getResolveConsecutive() {
        return resolveConsecutive;
    }
}

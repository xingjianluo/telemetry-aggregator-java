package com.github.xingjianluo.telemetryaggregatorjava.model;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * 单个 metric + 标签组合（series）在一个窗口内的聚合结论。
 * 所有字段由聚合引擎单线程更新；对外发布时为不可变快照。
 */
public final class SeriesAggregate {

    private final String metric;
    private final Map<String, String> tags;
    private long count;
    private double sum;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;

    public SeriesAggregate(String metric, Map<String, String> tags) {
        this.metric = metric;
        this.tags = Collections.unmodifiableMap(new TreeMap<>(tags));
    }

    /** 复制构造器：窗口关闭时生成脱离内部状态的防御性快照。 */
    public SeriesAggregate(SeriesAggregate other) {
        this.metric = other.metric;
        this.tags = other.tags;
        this.count = other.count;
        this.sum = other.sum;
        this.min = other.min;
        this.max = other.max;
    }

    public void add(double value) {
        count++;
        sum += value;
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public long getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    public double getMin() {
        return count == 0 ? 0.0 : min;
    }

    public double getMax() {
        return count == 0 ? 0.0 : max;
    }

    public double getAvg() {
        return count == 0 ? 0.0 : sum / count;
    }
}

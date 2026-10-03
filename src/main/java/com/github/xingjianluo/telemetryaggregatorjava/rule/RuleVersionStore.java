package com.github.xingjianluo.telemetryaggregatorjava.rule;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 规则版本历史（append-only）。
 *
 * <p>只在摄取 worker 单一线性化点写入：变更请求与事件写入在同一线程排队，
 * 从结构上杜绝“变更与事件同时发生时互相覆盖 / 状态丢失 / 重复计数”。
 * 版本历史不老化（条目数等于变更次数，运营场景下天然有界），
 * 供在线判定与按事件时间复盘共同读取，保证两者看到的规则依据一致。
 *
 * <p>生效边界（事件时间语义）：版本 v 的 {@code effectiveFrom} 由线性化时刻的
 * 水位线决定。窗口在 {@code windowEnd} 定稿时，选取 {@code effectiveFrom < windowEnd}
 * 的最新版本——边界之前已经定稿的窗口维持原判，边界之后定稿的窗口按新版本判定。
 */
public class RuleVersionStore {

    private final Map<String, List<RuleVersion>> history = new LinkedHashMap<>();
    private final AtomicLong versionCounter = new AtomicLong(0);

    public RuleVersionStore() {
    }

    /** 登记初始规则集（每个规则一个 version 递增、effectiveFrom=MIN_VALUE 的版本）。 */
    public synchronized void seed(List<RuleVersion> initialVersions) {
        if (!history.isEmpty() || versionCounter.get() > 0) {
            throw new IllegalStateException("规则历史已初始化，不能重复 seed");
        }
        for (RuleVersion v : List.copyOf(initialVersions)) {
            long num = versionCounter.incrementAndGet();
            RuleVersion seeded = new RuleVersion(num, v.id(), v.enabled(), v.scope(),
                    v.service(), v.metric(), v.tagSelector(), v.threshold(), v.operator(),
                    v.quietFromMillis(), v.quietUntilMillis(), v.effectiveFromMillis(),
                    v.createdAtMillis(), v.note());
            history.computeIfAbsent(v.id(), k -> new ArrayList<>()).add(seeded);
        }
    }

    /** 追加一个新版本；effectiveFromMillis 由线性化点按当前水位线给出。 */
    public synchronized RuleVersion append(String id, boolean enabled,
                                           AlertRule.Scope scope,
                                           String service, String metric,
                                           Map<String, String> tagSelector,
                                           double threshold, AlertRule.Operator operator,
                                           long quietFromMillis, long quietUntilMillis,
                                           long effectiveFromMillis, long createdAtMillis,
                                           String note) {
        long num = versionCounter.incrementAndGet();
        RuleVersion v = new RuleVersion(num, id, enabled, scope, service, metric,
                tagSelector == null ? Map.of() : Map.copyOf(tagSelector),
                threshold, operator, quietFromMillis, quietUntilMillis,
                effectiveFromMillis, createdAtMillis, note);
        history.computeIfAbsent(id, k -> new ArrayList<>()).add(v);
        return v;
    }

    /**
     * 某规则在窗口 windowEnd（事件时间）定稿时应使用的版本：
     * effectiveFrom &lt; windowEnd 的最新版本；不存在（规则在该窗口之后才创建）返回 null。
     */
    public synchronized RuleVersion versionAt(String ruleId, long windowEndMillis) {
        List<RuleVersion> versions = history.get(ruleId);
        if (versions == null) {
            return null;
        }
        RuleVersion found = null;
        for (RuleVersion v : versions) {
            if (v.effectiveFromMillis() < windowEndMillis) {
                found = v;
            }
        }
        return found;
    }

    /** 某规则的最新版本（在线判定/展示用）。 */
    public synchronized RuleVersion latest(String ruleId) {
        List<RuleVersion> versions = history.get(ruleId);
        return versions == null || versions.isEmpty() ? null : versions.get(versions.size() - 1);
    }

    /**
     * 复盘重建：按版本<b>原有版本号</b>直接登记版本，不重新编号，
     * 保证复盘解析出的版本号与告警事实中记录的版本号完全一致。
     * 传入序列必须按版本号升序（调用方按 seq 顺序喂入变更即可满足）。
     */
    public synchronized void registerExisting(RuleVersion v) {
        long num = v.version();
        if (num > versionCounter.get()) {
            versionCounter.set(num);
        }
        List<RuleVersion> list = history.computeIfAbsent(v.id(), k -> new ArrayList<>());
        list.add(v);
    }

    /** 截至事件时间 t 的全部规则最新版本（effectiveFrom &lt;= t；复盘用）。 */
    public synchronized List<RuleVersion> latestAsOf(long eventTimeMillis) {
        List<RuleVersion> out = new ArrayList<>();
        for (List<RuleVersion> versions : history.values()) {
            RuleVersion found = null;
            for (RuleVersion v : versions) {
                if (v.effectiveFromMillis() <= eventTimeMillis) {
                    found = v;
                }
            }
            if (found != null) {
                out.add(found);
            }
        }
        return List.copyOf(out);
    }

    /** 全部版本历史（按追加顺序）。 */
    public synchronized List<RuleVersion> history() {
        List<RuleVersion> out = new ArrayList<>();
        for (List<RuleVersion> versions : history.values()) {
            out.addAll(versions);
        }
        return List.copyOf(out);
    }

    /** 某规则的版本历史。 */
    public synchronized List<RuleVersion> historyOf(String ruleId) {
        List<RuleVersion> versions = history.get(ruleId);
        return versions == null ? List.of() : List.copyOf(versions);
    }

    /** 当前存在的规则 ID（按首次出现顺序）。 */
    public synchronized List<String> ruleIds() {
        return List.copyOf(history.keySet());
    }
}

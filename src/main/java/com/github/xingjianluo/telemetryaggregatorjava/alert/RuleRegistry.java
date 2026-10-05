package com.github.xingjianluo.telemetryaggregatorjava.alert;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 规则注册表：维护当前规则集与全部历史版本，支持在线变更（无需重启）。
 *
 * <p>生效边界语义：每次变更记录应用时刻的事件时间水位线
 * {@code effectiveFromMillis}。窗口结束时间 {@code end <= effectiveFromMillis}
 * 的窗口在变更前已定稿，维持原判；{@code end > effectiveFromMillis} 的窗口按新规则
 * 判定。实时与复盘共用 {@link #rulesForWindow(long)}，保证两侧判定一致。
 *
 * <p>历史上限：版本数超过 {@code maxVersions} 时，最老的变更被折叠进
 * “基线规则集”（baseline），并记录被淘汰的最老生效边界；边界及更早的窗口
 * 不再可精确复盘，由复盘服务显式拒绝（OUT_OF_RETENTION）。
 */
public class RuleRegistry {

    /** 单个规则的可变当前状态。 */
    private record MutableRule(AlertRule rule, long version, boolean enabled, long muteUntilMillis) {
        MutableRule with(AlertRule r, long v, boolean e, long m) {
            return new MutableRule(r, v, e, m);
        }
    }

    private final int maxVersions;
    /** 当前规则状态（ruleId -> 状态），仅供实时评估与展示。 */
    private final Map<String, MutableRule> current = new LinkedHashMap<>();
    /** 基线规则集：effectiveFrom <= baselineEffectiveFrom 的变更已折叠进来。 */
    private final Map<String, MutableRule> baseline = new LinkedHashMap<>();
    private long baselineEffectiveFrom = Long.MIN_VALUE;
    /** 基线之后的全部变更历史（版本、生效边界均单调不减）。 */
    private final List<RuleChange> history = new ArrayList<>();
    private long nextVersion = 1L;
    private Long oldestEvictedEffectiveFrom;
    /**
     * 幂等登记：requestId -> (原始请求, 已落版本)。同一 requestId 的重试直接复用
     * 已登记的变更记录，不再产生新版本；与历史一并受 maxVersions 保留约束，
     * 变更被折叠进基线时其幂等登记同步淘汰（与复盘保留范围一致）。
     */
    private final Map<String, RuleChangeRequest> idempotentRequests = new LinkedHashMap<>();
    private final Map<String, RuleChange> idempotentChanges = new LinkedHashMap<>();

    public RuleRegistry(List<AlertRule> initialRules, int maxVersions) {
        this.maxVersions = Math.max(1, maxVersions);
        for (AlertRule rule : initialRules) {
            MutableRule state = new MutableRule(rule, 0L, true, 0L);
            current.put(rule.id(), state);
            baseline.put(rule.id(), state);
        }
    }

    /**
     * 应用一次变更。必须由与摄取同一线性化点调用（worker 线程），
     * 调用前需保证 end <= effectiveFromMillis 的窗口均已定稿。
     *
     * <p>幂等：请求携带 {@code requestId} 时，若该键已登记，直接返回首次登记的
     * 变更记录（同一版本号、同一生效边界），不重复校验、不产生新版本、不改变
     * 规则状态；键相同但载荷不同视为冲突，快速拒绝且不产生版本。
     */
    public synchronized RuleChange applyChange(RuleChangeRequest request,
                                               long effectiveFromMillis, long nowMillis) {
        if (request != null && request.requestId() != null && !request.requestId().isBlank()) {
            RuleChange existing = idempotentChanges.get(request.requestId());
            if (existing != null) {
                RuleChangeRequest original = idempotentRequests.get(request.requestId());
                if (original != null && !original.samePayloadAs(request)) {
                    throw new IllegalArgumentException(
                            "requestId=" + request.requestId() + " 已登记为另一份载荷"
                                    + "（版本 " + existing.version() + "），相同幂等键不得复用于不同变更");
                }
                return existing;
            }
        }
        validate(request);
        String ruleId = request.ruleId();
        MutableRule before = current.get(ruleId);
        long version = nextVersion++;
        Long muteUntil = null;
        MutableRule after;
        switch (request.action()) {
            case UPSERT -> after = new MutableRule(request.rule(), version, true, 0L);
            case DISABLE -> after = before.with(before.rule(), version, false, before.muteUntilMillis());
            case ENABLE -> after = before.with(before.rule(), version, true, before.muteUntilMillis());
            case MUTE -> {
                muteUntil = request.muteUntilMillis();
                after = before.with(before.rule(), version, before.enabled(), muteUntil);
            }
            case UNMUTE -> after = before.with(before.rule(), version, before.enabled(), 0L);
            default -> throw new IllegalArgumentException("未知动作: " + request.action());
        }
        current.put(ruleId, after);
        RuleChange change = new RuleChange(version, ruleId, request.action(),
                after.rule(), muteUntil, effectiveFromMillis, nowMillis, request.requestId());
        history.add(change);
        if (request.requestId() != null && !request.requestId().isBlank()) {
            idempotentRequests.put(request.requestId(), request);
            idempotentChanges.put(request.requestId(), change);
        }
        enforceHistoryBound();
        return change;
    }

    /**
     * 仅校验不应用（供提交方在入队前快速失败）。权威校验仍在
     * {@link #applyChange} 内、与事件处理同一线性化点重新执行。
     */
    public synchronized void validateChange(RuleChangeRequest request) {
        validate(request);
    }

    private void validate(RuleChangeRequest request) {
        if (request == null || request.ruleId() == null || request.ruleId().isBlank()) {
            throw new IllegalArgumentException("ruleId 缺失");
        }
        if (request.action() == null) {
            throw new IllegalArgumentException("action 缺失");
        }
        switch (request.action()) {
            case UPSERT -> {
                if (request.rule() == null) {
                    throw new IllegalArgumentException("UPSERT 需要完整规则定义");
                }
                if (!request.ruleId().equals(request.rule().id())) {
                    throw new IllegalArgumentException("ruleId 与规则定义中的 id 不一致");
                }
            }
            case MUTE -> {
                requireKnown(request.ruleId());
                if (request.muteUntilMillis() == null || request.muteUntilMillis() < 0) {
                    throw new IllegalArgumentException("MUTE 需要非负的 muteUntilMillis（事件时间边界）");
                }
            }
            case DISABLE, ENABLE, UNMUTE -> requireKnown(request.ruleId());
            default -> throw new IllegalArgumentException("未知动作: " + request.action());
        }
    }

    private void requireKnown(String ruleId) {
        if (!current.containsKey(ruleId)) {
            throw new IllegalArgumentException("规则不存在: " + ruleId);
        }
    }

    /**
     * 解析窗口结束时间为 {@code windowEndMillis} 的窗口应当使用的规则集：
     * 基线 + 所有 {@code effectiveFrom < windowEnd} 的变更按版本序折叠。
     * 调用方（复盘）需先校验该窗口未超出可复盘保留范围。
     */
    public synchronized List<ResolvedRule> rulesForWindow(long windowEndMillis) {
        Map<String, MutableRule> state = new LinkedHashMap<>(baseline.size() + history.size());
        baseline.forEach((id, r) -> state.put(id, r));
        for (RuleChange change : history) {
            if (change.effectiveFromMillis() < windowEndMillis) {
                applyTo(state, change);
            }
        }
        List<ResolvedRule> resolved = new ArrayList<>();
        for (MutableRule r : state.values()) {
            if (r.enabled()) {
                resolved.add(new ResolvedRule(r.rule(), r.version(), r.muteUntilMillis()));
            }
        }
        return List.copyOf(resolved);
    }

    private void applyTo(Map<String, MutableRule> state, RuleChange change) {
        MutableRule before = state.get(change.ruleId());
        switch (change.action()) {
            case UPSERT -> state.put(change.ruleId(),
                    new MutableRule(change.ruleSnapshot(), change.version(), true, 0L));
            case DISABLE -> {
                if (before != null) {
                    state.put(change.ruleId(), before.with(before.rule(), change.version(), false,
                            before.muteUntilMillis()));
                }
            }
            case ENABLE -> {
                if (before != null) {
                    state.put(change.ruleId(), before.with(before.rule(), change.version(), true,
                            before.muteUntilMillis()));
                }
            }
            case MUTE -> {
                if (before != null) {
                    state.put(change.ruleId(), before.with(before.rule(), change.version(),
                            before.enabled(), change.muteUntilMillis() == null ? 0L
                                    : change.muteUntilMillis()));
                }
            }
            case UNMUTE -> {
                if (before != null) {
                    state.put(change.ruleId(), before.with(before.rule(), change.version(),
                            before.enabled(), 0L));
                }
            }
            default -> {
            }
        }
    }

    /** 版本数超限时，把最老变更折叠进基线并记录可复盘下界；幂等登记随之一并淘汰。 */
    private void enforceHistoryBound() {
        while (history.size() > maxVersions) {
            RuleChange evicted = history.remove(0);
            applyTo(baseline, evicted);
            baselineEffectiveFrom = Math.max(baselineEffectiveFrom, evicted.effectiveFromMillis());
            oldestEvictedEffectiveFrom = baselineEffectiveFrom;
            if (evicted.requestId() != null) {
                idempotentRequests.remove(evicted.requestId());
                idempotentChanges.remove(evicted.requestId());
            }
        }
    }

    /** 全部历史版本（基线之后的变更），按版本升序。 */
    public synchronized List<RuleChange> history() {
        return List.copyOf(history);
    }

    /** 当前生效规则（用于展示）。 */
    public synchronized List<ResolvedRule> currentRules() {
        List<ResolvedRule> out = new ArrayList<>();
        for (MutableRule r : current.values()) {
            out.add(new ResolvedRule(r.rule(), r.version(), r.muteUntilMillis()));
        }
        return List.copyOf(out);
    }

    /** 因版本历史上限被淘汰的最老生效边界；无淘汰返回 null。 */
    public synchronized Long oldestEvictedEffectiveFromMillis() {
        return oldestEvictedEffectiveFrom;
    }
}

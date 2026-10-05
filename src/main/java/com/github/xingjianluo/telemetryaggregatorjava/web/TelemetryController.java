package com.github.xingjianluo.telemetryaggregatorjava.web;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.ResolvedRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeOutcome;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionEngine;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.query.MetricsSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.query.QueryService;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceSnapshot;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 摄取与查询的 HTTP 入口。
 * 语义：每条事件独立给出结论；任一事件被拒绝时该条以 422 返回但不影响其它条目。
 */
@RestController
public class TelemetryController {

    private final IngestionEngine engine;
    private final QueryService queryService;

    public TelemetryController(IngestionEngine engine, QueryService queryService) {
        this.engine = engine;
        this.queryService = queryService;
    }

    @PostMapping("/events")
    public ResponseEntity<IngestResult> ingest(@RequestBody TelemetryEvent event) {
        IngestResult result = engine.ingest(event);
        return ResponseEntity.status(result.accepted() ? HttpStatus.ACCEPTED : HttpStatus.UNPROCESSABLE_ENTITY)
                .body(result);
    }

    @PostMapping("/events/batch")
    public ResponseEntity<List<IngestResult>> ingestBatch(@RequestBody List<TelemetryEvent> events) {
        List<IngestResult> results = new ArrayList<>(events.size());
        boolean anyRejected = false;
        for (TelemetryEvent event : events) {
            IngestResult result = engine.ingest(event);
            results.add(result);
            anyRejected |= !result.accepted();
        }
        return ResponseEntity.status(anyRejected ? HttpStatus.UNPROCESSABLE_ENTITY : HttpStatus.ACCEPTED)
                .body(results);
    }

    @GetMapping("/aggregates")
    public List<WindowAggregate> aggregates(@RequestParam(required = false) String service,
                                            @RequestParam(required = false) String metric) {
        return queryService.queryAggregates(service, metric);
    }

    @GetMapping("/alerts")
    public List<AlertSnapshot> alerts() {
        return queryService.queryAlerts();
    }

    @GetMapping("/traces/{traceId}")
    public TraceSnapshot trace(@PathVariable String traceId) {
        return queryService.queryTrace(traceId);
    }

    @GetMapping("/metrics")
    public MetricsSnapshot metrics() {
        return queryService.metrics();
    }

    /**
     * 在线变更告警规则（无需重启、不断流）。变更与事件写入在同一线性化点应用：
     * 先生效边界之前的已定稿窗口维持原判，之后按新规则判定。
     *
     * <p>幂等：请求体 {@code requestId} 或 {@code Idempotency-Key} 请求头提供幂等键
     * （二者均缺省时按请求内容生成，仅保证字节级重发幂等；客户端应显式携带）。
     * 同一键的任意次重试 / 并发重发只产生一个版本，返回同一个版本号与生效边界；
     * {@code replayed=true} 表示本次为重放（此前已生效，无副作用）。
     *
     * <p>失败可区分：422 RULE_CHANGE_INVALID=非法变更（确定无版本）；
     * 503 RULE_CHANGE_BACKPRESSURE=背压（确定未生效，用同键重试）；
     * 408 RULE_CHANGE_TIMEOUT=结论不确定（可能已生效，必须用同键重试）。
     */
    @PostMapping("/rules/changes")
    public ResponseEntity<?> changeRule(@RequestBody(required = false) RuleChangeRequestBody body,
                                        @RequestHeader(value = "Idempotency-Key", required = false)
                                        String idempotencyKey) {
        if (body == null) {
            return invalidChange("请求体为空或无法解析");
        }
        String requestId = body.requestId;
        if (requestId == null || requestId.isBlank()) {
            requestId = (idempotencyKey == null || idempotencyKey.isBlank())
                    ? body.contentKey() : idempotencyKey;
        }
        RuleChangeRequest request = new RuleChangeRequest(requestId, body.ruleId, body.action,
                body.rule, body.muteUntilMillis);
        try {
            RuleChangeOutcome outcome = engine.submitRuleChange(request);
            RuleChange c = outcome.change();
            // 顶层即版本字段（向后兼容），同时给出 requestId 与 replayed 语义。
            java.util.Map<String, Object> resp = new java.util.LinkedHashMap<>();
            resp.put("version", c.version());
            resp.put("requestId", c.requestId());
            resp.put("replayed", outcome.replayed());
            resp.put("ruleId", c.ruleId());
            resp.put("action", c.action());
            resp.put("ruleSnapshot", c.ruleSnapshot());
            resp.put("muteUntilMillis", c.muteUntilMillis());
            resp.put("effectiveFromMillis", c.effectiveFromMillis());
            resp.put("appliedAtMillis", c.appliedAtMillis());
            return ResponseEntity.ok(resp);
        } catch (IngestionEngine.RuleChangeInvalidException e) {
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(Map.of("error", "RULE_CHANGE_INVALID",
                            "requestId", String.valueOf(requestId),
                            "detail", String.valueOf(e.getMessage())));
        } catch (IngestionEngine.RuleChangeBackpressureException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "RULE_CHANGE_BACKPRESSURE",
                            "requestId", String.valueOf(requestId),
                            "applied", false,
                            "retryable", true,
                            "detail", String.valueOf(e.getMessage())));
        } catch (IngestionEngine.RuleChangeTimeoutException e) {
            return ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT)
                    .body(Map.of("error", "RULE_CHANGE_TIMEOUT",
                            "requestId", String.valueOf(requestId),
                            "applied", "UNKNOWN",
                            "retryable", true,
                            "detail", String.valueOf(e.getMessage())));
        }
    }

    private ResponseEntity<java.util.Map<String, String>> invalidChange(String detail) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(Map.of("error", "RULE_CHANGE_INVALID", "detail", detail));
    }

    /** 规则变更请求体（幂等键可在体中或通过 Idempotency-Key 头携带）。 */
    public static final class RuleChangeRequestBody {
        public String requestId;
        public String ruleId;
        public RuleChange.Action action;
        public com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule rule;
        public Long muteUntilMillis;

        /** 按请求内容生成稳定键：仅保证“字节级完全相同的重发”幂等。 */
        String contentKey() {
            String raw = "auto:" + ruleId + '|' + action + '|' + muteUntilMillis + '|'
                    + (rule == null ? ""
                    : String.join("|", rule.id(), String.valueOf(rule.scope()),
                            String.valueOf(rule.service()), String.valueOf(rule.metric()),
                            String.valueOf(rule.tagSelector()),
                            String.valueOf(rule.threshold()), String.valueOf(rule.operator())));
            return Integer.toHexString(raw.hashCode());
        }
    }

    /** 当前生效规则集（含版本与免打扰边界）。 */
    @GetMapping("/rules")
    public List<ResolvedRule> rules() {
        return queryService.currentRules();
    }

    /** 全部规则变更历史（版本升序，含每次的生效边界）。 */
    @GetMapping("/rules/history")
    public List<RuleChange> ruleHistory() {
        return queryService.ruleHistory();
    }

    /**
     * 按事件时间边界复盘：返回该边界处已定稿的窗口聚合、当时生效的规则版本、
     * 告警状态与调用链结论。超出保留范围时 status=OUT_OF_RETENTION 并说明原因。
     */
    @GetMapping("/replay")
    public ResponseEntity<ReplayReport> replay(@RequestParam long asOfMillis) {
        try {
            return ResponseEntity.ok(queryService.replay(asOfMillis));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).build();
        }
    }

    /** 请求体无法解析等：返回结构化拒绝，而不是静默 500。 */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<IngestResult> handle(Exception e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(IngestResult.rejected("<bad-request>", RejectReason.INVALID_EVENT,
                        e.getClass().getSimpleName() + ": " + e.getMessage()));
    }
}

package com.github.xingjianluo.telemetryaggregatorjava.web;

import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionEngine;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeResult;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 规则在线运营入口：不重启、不断流调整阈值 / 匹配范围 / 启停 / 免打扰区间。
 * 所有变更都有可查询的版本号与生效边界；相同 requestId 的重试幂等。
 */
@RestController
public class RuleAdminController {

    private final IngestionEngine engine;

    public RuleAdminController(IngestionEngine engine) {
        this.engine = engine;
    }

    /** 提交一次规则变更；200=已应用/幂等重放，422=非法请求，503=缓冲满，504=处理超时。 */
    @PostMapping("/rules")
    public ResponseEntity<RuleChangeResult> change(@RequestBody RuleChangeRequest request) {
        RuleChangeResult result = engine.changeRule(request);
        HttpStatus status = switch (result.status()) {
            case APPLIED, DUPLICATE_REQUEST -> HttpStatus.OK;
            case INVALID_REQUEST -> HttpStatus.UNPROCESSABLE_ENTITY;
            case BUFFER_FULL -> HttpStatus.SERVICE_UNAVAILABLE;
            case PROCESSING_TIMEOUT -> HttpStatus.GATEWAY_TIMEOUT;
        };
        return ResponseEntity.status(status).body(result);
    }

    /** 全部规则版本历史（审计）。 */
    @GetMapping("/rules/history")
    public List<RuleVersion> history() {
        return engine.ruleStore().history();
    }

    /** 单条规则的版本历史与生效边界。 */
    @GetMapping("/rules/{ruleId}/history")
    public List<RuleVersion> historyOf(@PathVariable String ruleId) {
        return engine.ruleStore().historyOf(ruleId);
    }

    /** 请求体无法解析等：返回结构化非法请求结论，而不是静默 500。 */
    @org.springframework.web.bind.annotation.ExceptionHandler(Exception.class)
    public ResponseEntity<RuleChangeResult> handle(Exception e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new RuleChangeResult(null, "<bad-request>",
                        com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeStatus.INVALID_REQUEST,
                        null, null,
                        e.getClass().getSimpleName() + ": " + e.getMessage()));
    }
}

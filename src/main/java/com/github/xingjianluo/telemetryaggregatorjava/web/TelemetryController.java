package com.github.xingjianluo.telemetryaggregatorjava.web;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionEngine;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

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

    /** 请求体无法解析等：返回结构化拒绝，而不是静默 500。 */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<IngestResult> handle(Exception e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(IngestResult.rejected("<bad-request>", RejectReason.INVALID_EVENT,
                        e.getClass().getSimpleName() + ": " + e.getMessage()));
    }
}

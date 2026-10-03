package com.github.xingjianluo.telemetryaggregatorjava.web;

import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 按事件时间复盘入口。纯只读重放，不改写实时状态；
 * 超出保留范围返回 404 + NOT_REPLAYABLE，资源受限返回 503。
 */
@RestController
public class ReplayController {

    private final ReplayService replayService;

    public ReplayController(ReplayService replayService) {
        this.replayService = replayService;
    }

    @GetMapping("/replay")
    public ResponseEntity<ReplayReport> replay(
            @RequestParam("at") long atMillis,
            @RequestParam(value = "traceId", required = false) String traceId,
            @RequestParam(value = "service", required = false) String service,
            @RequestParam(value = "metric", required = false) String metric) {
        ReplayService.ServiceMetric filter = (service == null && metric == null)
                ? null : new ReplayService.ServiceMetric(service, metric);
        ReplayReport report = replayService.replayAt(atMillis, traceId, filter);
        HttpStatus status = switch (report.status()) {
            case COMPLETE, PARTIAL -> HttpStatus.OK;
            case NOT_REPLAYABLE -> HttpStatus.NOT_FOUND;
            case RESOURCE_LIMITED -> HttpStatus.SERVICE_UNAVAILABLE;
        };
        return ResponseEntity.status(status).body(report);
    }
}

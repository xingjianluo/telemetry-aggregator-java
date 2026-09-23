package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端：经 HTTP 摄取 -> 聚合/告警/链路/指标查询，验证各部分语义对齐。
 * 关闭空闲推进，水位线只随事件时间前进，保证可复现。
 */
@SpringBootTest(properties = {
        "telemetry.window.size-millis=10",
        "telemetry.window.allowed-lateness-millis=5",
        "telemetry.window.advance-on-idle=false",
        "telemetry.alert.breach-streak=1",
        "telemetry.alert.recover-streak=1",
        "telemetry.alert.cooldown=0",
        "telemetry.alert.rules=e2e-high,TAGS,svc-e2e,m,10,GT"
})
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class EndToEndIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(EndToEndIntegrationTest.class);
    private final ObjectMapper json = new ObjectMapper();

    @Autowired
    private MockMvc mvc;

    private String eventJson(String id, long t, String trace, String span, String parent,
                             double value, Map<String, String> tags) throws Exception {
        return json.writeValueAsString(Map.of(
                "eventId", id,
                "eventTimeMillis", t,
                "traceId", trace,
                "spanId", span,
                "parentSpanId", parent == null ? "" : parent,
                "service", "svc-e2e",
                "metric", "m",
                "value", value,
                "tags", tags == null ? Map.of() : tags));
    }

    private String eventJsonSeal(String id, long t) throws Exception {
        return json.writeValueAsString(Map.of(
                "eventId", id,
                "eventTimeMillis", t,
                "traceId", "tr-seal",
                "spanId", "s",
                "parentSpanId", "",
                "service", "svc-seal",
                "metric", "other",
                "value", 0,
                "tags", Map.of()));
    }

    @Test
    void ingestAggregateAlertTraceAndMetricsAreAligned() throws Exception {
        log.info("[E2E] 通过 HTTP 发送同一 trace 的父子两 span（乱序：child 先到）");
        String child = eventJson("c1", 1, "tr-e2e", "child", "root", 20, Map.of("host", "h1"));
        log.info("[E2E] POST /events body={}", child);
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(child))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        // 乱序补齐 root
        String root = eventJson("r1", 0, "tr-e2e", "root", null, 5, Map.of("host", "h1"));
        log.info("[E2E] POST /events body={}", root);
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(root))
                .andExpect(status().isAccepted());

        // L=5：seal 事件 t=15 => wm=10，窗口[0,10)关闭定稿；其本身落入窗口[10,20)
        // 均值 (20+5)/2=12.5 > 10 => breachStreak=1 => FIRING
        String seal = eventJsonSeal("seal", 15);
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(seal))
                .andExpect(status().isAccepted());

        // 给窗口 10 一个恢复窗口后再关闭（这里只验证已 FIRING 的存在性即可）
        log.info("[E2E] GET /aggregates?service=svc-e2e");
        String aggregates = mvc.perform(get("/aggregates").param("service", "svc-e2e"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        log.info("[E2E] aggregates={}", aggregates);
        JsonNode aggNode = json.readTree(aggregates);
        org.junit.jupiter.api.Assertions.assertTrue(aggNode.isArray());
        boolean windowZeroSealed = false;
        for (JsonNode a : aggNode) {
            if (a.get("windowStartMillis").asLong() == 0L) {
                windowZeroSealed = true;
                org.junit.jupiter.api.Assertions.assertEquals(2, a.get("count").asLong());
                org.junit.jupiter.api.Assertions.assertEquals(25.0, a.get("sum").asDouble(), 1e-9);
                org.junit.jupiter.api.Assertions.assertEquals(12.5, a.get("sum").asDouble()
                        / a.get("count").asLong(), 1e-9);
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(windowZeroSealed, "窗口0应已定稿可查");

        log.info("[E2E] GET /alerts");
        String alerts = mvc.perform(get("/alerts")).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        log.info("[E2E] alerts={}", alerts);
        org.junit.jupiter.api.Assertions.assertTrue(alerts.contains("FIRING"),
                "均值12.5>10 且 breachStreak=1，应存在 FIRING 告警");

        log.info("[E2E] GET /traces/tr-e2e");
        String trace = mvc.perform(get("/traces/tr-e2e")).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        log.info("[E2E] trace={}", trace);
        org.junit.jupiter.api.Assertions.assertTrue(trace.contains("COMPLETE"),
                "乱序补齐后链路应还原为 COMPLETE");

        log.info("[E2E] GET /metrics");
        String metrics = mvc.perform(get("/metrics")).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        log.info("[E2E] metrics={}", metrics);
        JsonNode mNode = json.readTree(metrics);
        org.junit.jupiter.api.Assertions.assertTrue(mNode.get("acceptedCount").asLong() >= 3);
        org.junit.jupiter.api.Assertions.assertTrue(mNode.has("rejectedByReason"));
        org.junit.jupiter.api.Assertions.assertTrue(mNode.has("queueDepth"));
    }

    @Test
    void duplicatesAndLateEventsReturnExplicitReasonsOverHttp() throws Exception {
        log.info("[E2E] 重复投递返回相同首次结论；迟到事件带明确拒绝原因（HTTP 422）");
        String e = eventJson("dup-e2e", 5, "trx", "s1", null, 1, Map.of());
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(e))
                .andExpect(status().isAccepted());
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(e))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        // 推进水位线后再发极早事件
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON)
                        .content(eventJson("adv", 100, "tra", "s", null, 1, Map.of())))
                .andExpect(status().isAccepted());
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON)
                        .content(eventJson("toolate", 80, "trb", "s", null, 1, Map.of())))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.rejectReason").value("LATE_BEHIND_WATERMARK"));
    }
}

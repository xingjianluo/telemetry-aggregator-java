package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP 层冒烟：规则在线变更、版本历史查询、按事件时间复盘。
 */
@SpringBootTest(properties = {
        "telemetry.window.size-millis=10",
        "telemetry.window.allowed-lateness-millis=0",
        "telemetry.window.advance-on-idle=false",
        "telemetry.alert.breach-streak=1",
        "telemetry.alert.recover-streak=1",
        "telemetry.alert.cooldown=0",
        "telemetry.alert.rules=r,TAGS,svc,m,10,GT"
})
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RuleReplayHttpTest {

    private static final Logger log = LoggerFactory.getLogger(RuleReplayHttpTest.class);
    private final ObjectMapper json = new ObjectMapper();

    @Autowired
    private MockMvc mvc;

    private String event(String id, long t, double value) throws Exception {
        return json.writeValueAsString(Map.of(
                "eventId", id, "eventTimeMillis", t, "traceId", "tr-" + id,
                "spanId", "s", "parentSpanId", "", "service", "svc",
                "metric", "m", "value", value, "tags", Map.of()));
    }

    @Test
    void ruleChangeAndReplayOverHttp() throws Exception {
        log.info("[CASE] HTTP：投递 -> FIRING -> 在线调阈值 -> 复盘两个边界");
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(event("e1", 1, 20)))
                .andExpect(status().isAccepted());
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(event("s1", 11, 0)))
                .andExpect(status().isAccepted());
        mvc.perform(get("/alerts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].state").value("FIRING"))
                .andExpect(jsonPath("$[0].ruleVersion").value(0));

        // 在线把阈值调到 100（无需重启）
        String change = json.writeValueAsString(Map.of(
                "ruleId", "r", "action", "UPSERT",
                "rule", Map.of("id", "r", "scope", "TAGS", "service", "svc", "metric", "m",
                        "tagSelector", Map.of(), "threshold", 100.0, "operator", "GT")));
        mvc.perform(post("/rules/changes").contentType(MediaType.APPLICATION_JSON).content(change))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.effectiveFromMillis").value(11));

        // 版本历史可查询
        mvc.perform(get("/rules/history"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].version").value(1))
                .andExpect(jsonPath("$[0].action").value("UPSERT"));
        mvc.perform(get("/rules"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].version").value(1))
                .andExpect(jsonPath("$[0].rule.threshold").value(100.0));

        // 新窗口按新阈值判定：20 <= 100 => OK
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(event("e2", 21, 20)))
                .andExpect(status().isAccepted());
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(event("s2", 31, 0)))
                .andExpect(status().isAccepted());
        mvc.perform(get("/alerts"))
                .andExpect(jsonPath("$[0].state").value("OK"))
                .andExpect(jsonPath("$[0].ruleVersion").value(1));

        // 复盘边界 asOf=11：维持原判 FIRING/v0；asOf=31：OK/v1
        mvc.perform(get("/replay").param("asOfMillis", "11"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("OK"))
                .andExpect(jsonPath("$.alerts[0].state").value("FIRING"))
                .andExpect(jsonPath("$.alerts[0].ruleVersion").value(0));
        mvc.perform(get("/replay").param("asOfMillis", "31"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.alerts[0].state").value("OK"))
                .andExpect(jsonPath("$.alerts[0].ruleVersion").value(1))
                .andExpect(jsonPath("$.ruleChanges[0].version").value(1));
        log.info("[ASSERT] HTTP 规则变更 / 历史 / 复盘全部符合预期");

        // 非法变更：422 + 可区分错误
        String bad = json.writeValueAsString(Map.of(
                "ruleId", "ghost", "action", "DISABLE"));
        mvc.perform(post("/rules/changes").contentType(MediaType.APPLICATION_JSON).content(bad))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("RULE_CHANGE_REJECTED"));
    }
}

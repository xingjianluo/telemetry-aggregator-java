package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
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

import java.util.LinkedHashMap;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 经 HTTP 的规则在线运营 + 按事件时间复盘端到端验证：
 * POST /rules 在线调整阈值并产生可查询版本；GET /replay 给出与实时一致的结论；
 * 超保留范围返回 404 + NOT_REPLAYABLE。
 */
@SpringBootTest(properties = {
        "telemetry.window.size-millis=10",
        "telemetry.window.allowed-lateness-millis=0",
        "telemetry.window.advance-on-idle=false",
        "telemetry.alert.breach-streak=1",
        "telemetry.alert.recover-streak=1",
        "telemetry.alert.cooldown=0",
        "telemetry.alert.rules=http-high,TAGS,svc,m,100,GT",
        "telemetry.replay.max-journal-events=4"
})
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RuleReplayHttpIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RuleReplayHttpIntegrationTest.class);
    private final ObjectMapper json = new ObjectMapper();

    @Autowired
    private MockMvc mvc;

    private String event(String id, long t, String metric, double value) throws Exception {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", id);
        m.put("eventTimeMillis", t);
        m.put("traceId", "tr-" + id);
        m.put("spanId", "s");
        m.put("parentSpanId", "");
        m.put("service", "svc");
        m.put("metric", metric);
        m.put("value", value);
        m.put("tags", Map.of());
        return json.writeValueAsString(m);
    }

    @Test
    void onlineRuleChangeThenReplayMatchesLive() throws Exception {
        // 窗口0：m=50，旧阈值100 => 不报警；seal t=11 关闭窗口0。
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(event("a0", 1, "m", 50)))
                .andExpect(status().isAccepted());
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(event("f0", 11, "z", 0)))
                .andExpect(status().isAccepted());

        // 在线把阈值调到 10（不重启）。
        String change = json.writeValueAsString(Map.of(
                "requestId", "http-chg-1",
                "id", "http-high",
                "threshold", 10,
                "note", "HTTP在线降阈值"));
        log.info("[HTTP] POST /rules {}", change);
        String chgResp = mvc.perform(post("/rules").contentType(MediaType.APPLICATION_JSON).content(change))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPLIED"))
                .andExpect(jsonPath("$.version").value(2))
                .andExpect(jsonPath("$.effectiveFromMillis").value(11))
                .andReturn().getResponse().getContentAsString();
        log.info("[HTTP] change result={}", chgResp);

        // 相同 requestId 重试：不产生新版本。
        mvc.perform(post("/rules").contentType(MediaType.APPLICATION_JSON).content(change))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value(2));

        // 版本历史可查询。
        String history = mvc.perform(get("/rules/http-high/history"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        org.junit.jupiter.api.Assertions.assertTrue(history.contains("\"version\":1"));
        org.junit.jupiter.api.Assertions.assertTrue(history.contains("\"version\":2"));
        log.info("[HTTP] version history={}", history);

        // 窗口20：m=50，新阈值10 => FIRING。
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(event("a20", 21, "m", 50)))
                .andExpect(status().isAccepted());
        mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(event("f20", 31, "z", 0)))
                .andExpect(status().isAccepted());
        String alerts = mvc.perform(get("/alerts")).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        org.junit.jupiter.api.Assertions.assertTrue(alerts.contains("FIRING"));
        org.junit.jupiter.api.Assertions.assertTrue(alerts.contains("\"ruleVersion\":2"));

        // 复盘 T=31：与实时一致 FIRING；且边界 T=11 时窗口0按 v1 为 OK。
        String replay31 = mvc.perform(get("/replay").param("at", "31"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETE"))
                .andReturn().getResponse().getContentAsString();
        log.info("[HTTP] replay@31={}", replay31);
        JsonNode r31 = json.readTree(replay31);
        org.junit.jupiter.api.Assertions.assertTrue(replay31.contains("FIRING"));
        org.junit.jupiter.api.Assertions.assertTrue(r31.get("basis").size() > 0,
                "复盘必须给出判定依据");

        String replay11 = mvc.perform(get("/replay").param("at", "11"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        JsonNode r11 = json.readTree(replay11);
        JsonNode r11Alert = r11.get("alerts").get(0);
        org.junit.jupiter.api.Assertions.assertEquals("OK", r11Alert.get("state").asText());
        org.junit.jupiter.api.Assertions.assertEquals(1L, r11Alert.get("ruleVersion").asLong());
        log.info("[HTTP] replay@11 state=OK ruleVersion=1（边界之前维持原判）");
    }

    @Test
    void invalidRuleChangeReturns422AndOldReplayReturns404() throws Exception {
        // 倒置免打扰区间 => 422
        String bad = json.writeValueAsString(Map.of(
                "id", "http-high",
                "threshold", 5,
                "quietFromMillis", 100,
                "quietUntilMillis", 50));
        mvc.perform(post("/rules").contentType(MediaType.APPLICATION_JSON).content(bad))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.status").value("INVALID_REQUEST"));

        // 日志容量=4，塞入足够多事件老化掉早期数据，再复盘很早时间 => 404 NOT_REPLAYABLE。
        long[] times = {1, 11, 21, 31, 41, 51, 61, 71, 81, 91};
        for (int i = 0; i < times.length; i++) {
            mvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON)
                            .content(event("j" + i, times[i], i % 2 == 0 ? "m" : "z", i % 2 == 0 ? 50 : 0)))
                    .andExpect(status().isAccepted());
        }
        String resp = mvc.perform(get("/replay").param("at", "11"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value("NOT_REPLAYABLE"))
                .andReturn().getResponse().getContentAsString();
        log.info("[HTTP] replay@11 beyond retention => {}", resp);
        org.junit.jupiter.api.Assertions.assertTrue(resp.contains("可复盘下界"));
    }
}

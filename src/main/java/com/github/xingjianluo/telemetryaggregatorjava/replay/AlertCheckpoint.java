package com.github.xingjianluo.telemetryaggregatorjava.replay;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;

import java.util.List;

/**
 * 复盘日志老化（最老事件被丢弃）时刻的告警状态机检查点。
 *
 * <p>有了检查点，早于保留下界之后的复盘仍能正确延续 breach/recovery 计数与状态；
 * 早于检查点水位线的复盘请求则明确返回 NOT_REPLAYABLE。
 */
public record AlertCheckpoint(
        long watermarkMillis,
        List<AlertEngine.SubjectState> subjects
) {
}

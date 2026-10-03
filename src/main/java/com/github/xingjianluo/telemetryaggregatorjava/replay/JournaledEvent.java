package com.github.xingjianluo.telemetryaggregatorjava.replay;

import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;

/**
 * 进入复盘日志的一条已接受事件。
 *
 * <p>{@code seq} 是全局线性化序列号（与规则变更共用同一序列空间），
 * 固定事件时间相同时的处理顺序；{@code watermarkAfterMillis} 是处理完该事件后的
 * 水位线，用于把“给我一个时间点”精确还原为离散的线性化切点，
 * 使复盘结论只由输入事件 + 规则变更历史决定，与执行时刻 / 机器无关。
 */
public record JournaledEvent(
        long seq,
        TelemetryEvent event,
        long windowStartMillis,
        long watermarkAfterMillis
) {
}

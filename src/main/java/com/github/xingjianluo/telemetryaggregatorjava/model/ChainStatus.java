package com.github.xingjianluo.telemetryaggregatorjava.model;

/**
 * 调用链判定状态。链路不完整或成环都不会导致链路被丢弃或判定中断，
 * 而是显式标记为下列状态之一。
 */
public enum ChainStatus {
    /** 根 span 已到达且父引用全部闭合，结构完整。 */
    COMPLETE,
    /** 已到 span 的父引用尚未到达（孤立），等待补全。 */
    AWAITING_PARENT,
    /** 父引用形成有向环路。 */
    CYCLE,
    /** 已到 span 的父 id 指向一个已被水位线剪枝的 trace（迟到且不可闭合）。 */
    DISCONNECTED
}

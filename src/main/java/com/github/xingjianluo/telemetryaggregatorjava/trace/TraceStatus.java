package com.github.xingjianluo.telemetryaggregatorjava.trace;

/**
 * 调用链拓扑还原结论。异常链路被明确标记而非丢弃。
 */
public enum TraceStatus {
    /** 恰好一个根 span，所有 span 连通。 */
    COMPLETE,
    /** 存在 parentSpanId 指向链内不存在 span 的节点（可能乱序未到齐）。 */
    ORPHAN,
    /** 父子边构成有向环。 */
    CYCLE,
    /** 同一 spanId 出现多次（标识冲突的另一种表现）。 */
    DUPLICATE_SPAN
}

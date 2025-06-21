package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.ThreadUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

/**
 * 背景：
 *      在flink Event Time时间语义场景，会出现没有持续数据，watermark不推进，导致计算不触发的问题。flink官方只提供了数据源多分区场景，将分区标记为空闲分区的功能，解决空闲分区不影响其他有数据分区watermark推进的问题。
 * 解决方案：
 *      自定义水印策略，采用类似心跳机制的方式。可配置最大空闲时间（maxIdleTimeMillis）,超过最大空闲时长，没有更新时间戳，就发送一个新的水印（lastEmittedWatermark + maxIdleTimeMillis）
 * 注意事项：
 *      如果设置withIdleness(Duration.ofSeconds(xxx))策略，会导致此方案失效；具体原因是设置withIdleness之后，代码会强制走WatermarksWithIdleness.onPeriodicEmit，而不再走用户自定义的水印策略
 *  @param <T>
 */
public class CustomPeriodicWatermarkGenerator<T> implements WatermarkGenerator<T>, CheckpointedFunction {
    private final long maxIdleTimeMillis; // 最大空闲时间，例如 1 分钟，可配置
    // 记录最后一次发出的水印
    private long lastEmittedWatermark = Long.MIN_VALUE;
    // 记录当前最大的事件时间
    private long currentMaxTimestamp = Long.MIN_VALUE;
    // 更新水印时间
    private long updateWatermarkTime = Long.MAX_VALUE;

    private final long maxOutOfOrderness; // 5 秒钟

    // 用于状态保存的句柄
    private transient ListState<Long> checkpointedState;

    public CustomPeriodicWatermarkGenerator(long maxIdleTimeMillis, long maxOutOfOrderness) {
        this.maxIdleTimeMillis = maxIdleTimeMillis;
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    /**
     * 当每条事件到达时调用，更新当前最大事件时间，并在第一次更新时发出水印。
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        // 更新当前最大事件时间
        if (currentMaxTimestamp < eventTimestamp) {
            updateWatermarkTime();
            currentMaxTimestamp = eventTimestamp;
        }

        // 第一次收到事件后，直接发出水印
        if (lastEmittedWatermark == Long.MIN_VALUE) {
            lastEmittedWatermark = currentMaxTimestamp;
            output.emitWatermark(new Watermark(lastEmittedWatermark - maxOutOfOrderness));
            updateWatermarkTime();
        }
    }

    /**
     * 定期调用，如果在一段时间内没有新事件导致水印提升，则推进水印。
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        ThreadUtils.printThreadStackTrace();
        long currentTime = System.currentTimeMillis();
        long candidateWatermark;
        if ((currentMaxTimestamp == lastEmittedWatermark)
                && (currentTime - updateWatermarkTime >= maxIdleTimeMillis)) {
            // 超过最大时间，没有更新时间戳，就发送一个新的水印
            candidateWatermark = lastEmittedWatermark + maxIdleTimeMillis;
            currentMaxTimestamp = candidateWatermark;
            updateWatermarkTime();
        } else {
            candidateWatermark = currentMaxTimestamp;
        }
        lastEmittedWatermark = candidateWatermark;
        System.out.println("lastEmittedWatermark:"+lastEmittedWatermark);
        output.emitWatermark(new Watermark(candidateWatermark - maxOutOfOrderness));
    }

    //更新计时
    public void updateWatermarkTime() {
        this.updateWatermarkTime = System.currentTimeMillis();
    }

    // 以下两个方法用于状态的快照和恢复，保证在故障恢复后水印逻辑能继续正确运行

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 清空之前的状态，并保存最新的 lastEmittedWatermark
        checkpointedState.clear();
        checkpointedState.add(lastEmittedWatermark);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> descriptor =
                new ListStateDescriptor<>("watermarkState", Long.class);
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            // 恢复上次保存的 watermak 状态
            for (Long state : checkpointedState.get()) {
                lastEmittedWatermark = state;
            }
        }
    }
}
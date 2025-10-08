package com.flink.tutorials.java.chapter5_time;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.util.Preconditions;
import java.time.Duration;
import java.util.Date;

/**
 * https://icnua45rtu0j.feishu.cn/docx/MxWMdbGiyo6rv0xmRgzcMuOPnBJ
 * 背景：
 *      在flink Event Time时间语义场景，会出现没有持续数据，watermark不推进，导致计算不触发的问题。flink官方只提供了数据源多分区场景，将分区标记为空闲分区的功能，解决空闲分区不影响其他有数据分区watermark推进的问题。
 * 解决方案：
 *      自定义水印策略，采用类似心跳机制的方式。可配置最大空闲时间（sourceIdleness）,超过最大空闲时长，没有更新时间戳，就发送一个新的水印maxTimestamp （也就是lastEmittedWatermark + maxOutOfOrderness +1）
 * 注意事项：
 *      如果设置withIdleness(Duration.ofSeconds(xxx))策略，会导致此方案失效；具体原因是设置withIdleness之后，代码会强制走WatermarksWithIdleness.onPeriodicEmit，而不再走用户自定义的水印策略
 *  @param <T>
 */
public class CustomPeriodicWatermarkGenerator<T> implements WatermarkGenerator<T> {
    private final long sourceIdlenessMillis; // 最大空闲时间，例如 1 分钟，可配置
    // 记录最后一次发出的水印
    private long lastEmittedWatermark = Long.MIN_VALUE;
    // 记录当前最大的事件时间
    private long maxTimestamp;
    // 记录更新水印时间
    private long watermarkUpdateTime = Long.MAX_VALUE;

    private final long maxOutOfOrdernessMillis; // 5 秒钟

    public CustomPeriodicWatermarkGenerator(Duration sourceIdleness, Duration maxOutOfOrderness) {
        Preconditions.checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        Preconditions.checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");
        Preconditions.checkNotNull(sourceIdleness, "sourceIdleness");
        Preconditions.checkArgument(!sourceIdleness.isNegative(), "sourceIdleness cannot be negative");
        this.sourceIdlenessMillis = sourceIdleness.toMillis();
        this.maxOutOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.maxTimestamp = Long.MIN_VALUE + this.maxOutOfOrdernessMillis + 1;
    }

    /**
     * 当每条事件到达时调用，更新当前最大事件时间，并在第一次更新时发出水印。
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        // 更新当前最大事件时间
        if (maxTimestamp < eventTimestamp) {
            maxTimestamp = eventTimestamp;
        }
        watermarkUpdateTime();
    }

    /**
     * 定期调用，如果在一段时间内没有新事件导致水印提升，则推进水印。
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 最后的-1是为了和flink原生提供的BoundedOutOfOrdernessWatermarks水印策略保持一致，类似窗口中的左闭右开
        long candidateWatermark = maxTimestamp - maxOutOfOrdernessMillis - 1;

        if (candidateWatermark > lastEmittedWatermark) {
            lastEmittedWatermark = candidateWatermark;
        } else if ((System.currentTimeMillis() - watermarkUpdateTime) >= sourceIdlenessMillis) {
            // 超过最大空闲时间，没有更新水印，就把原来的水印向前推进sourceIdlenessMillis + 1，解决数据源空闲场景，未完成CEP计算无法触发问题
            candidateWatermark = maxTimestamp;
            lastEmittedWatermark = candidateWatermark;
            String logInfo = StrUtil.format("Source Idleness increment watermark:{}", DateUtil.format(new Date(candidateWatermark), DatePattern.NORM_DATETIME_MS_PATTERN));
            System.out.println(logInfo);
        }

        output.emitWatermark(new Watermark(candidateWatermark));
    }

    // 记录waterMark更新时间
    public void watermarkUpdateTime() {
        this.watermarkUpdateTime = System.currentTimeMillis();
    }
}
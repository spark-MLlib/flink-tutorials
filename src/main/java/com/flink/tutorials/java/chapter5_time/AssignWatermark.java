package com.flink.tutorials.java.chapter5_time;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.flink.tutorials.java.bean.Event;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import java.util.Date;

public class AssignWatermark {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 访问 http://localhost:8082 可以看到Flink Web UI
        conf.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, conf);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 每隔一定时间生成一个Watermark
        env.getConfig().setAutoWatermarkInterval(1000L);
        DataStream<Event> input = env.addSource(new EventSourceFunction());
        DataStream<Event> watermark = input.assignTimestampsAndWatermarks(
//                WatermarkStrategy.forGenerator((context -> new MyPeriodicGenerator()))
                WatermarkStrategy.forGenerator((context -> new MyPunctuatedGenerator()))
                        .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
        );

        printWaterMark(watermark);
        env.execute("periodic and punctuated watermark");
    }

    private static void printWaterMark(DataStream<Event> watermark) {
        watermark.process(new ProcessFunction<Event, Long>() {

            @Override
            public void processElement(Event value, ProcessFunction<Event, Long>.Context ctx, Collector<Long> out) throws Exception {
                long currentWatermark = ctx.timerService().currentWatermark();
                String formatted = DateUtil.format(new Date(currentWatermark), DatePattern.NORM_DATETIME_MS_PATTERN);
                System.out.println("currentWatermark:" + formatted);
            }
        });
    }

    // 定期生成Watermark
    public static class MyPeriodicGenerator implements WatermarkGenerator<Event> {

        private final long maxOutOfOrderness = 5 * 1000; // 5 秒钟
        private long currentMaxTimestamp;                 // 已抽取的Timestamp最大值

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 更新currentMaxTimestamp为当前遇到的最大值
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Watermark比currentMaxTimestamp最大值慢
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness));
        }

    }

    // 逐个检查数据流中的元素，根据元素中的特殊字段，判断是否要生成Watermark
    public static class MyPunctuatedGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new Watermark(event.getTimestamp()));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 这里不需要做任何事情，因为我们在 onEvent() 方法中生成了Watermark
        }

    }

    // 自定义事件源函数，模拟生成事件流
    public static class EventSourceFunction implements SourceFunction<Event> {

        @Override
        public void run(SourceContext<Event> ctx) throws InterruptedException {
            // 模拟事件按顺序发送
            ctx.collect(new Event(1736991168000L)); //2025-01-16 09:32:48
            mockSleep();
            ctx.collect(new Event(1736991348000L)); //2025-01-16 09:35:48
            mockSleep();
            ctx.collect(new Event(1736991588000L)); //2025-01-16 09:39:48
            while (true) {}
        }

        private void mockSleep() throws InterruptedException {
            long sleep = 1000L;
            Thread.sleep(sleep);
        }

        @Override
        public void cancel() {

        }
    }
}

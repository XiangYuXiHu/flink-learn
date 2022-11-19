package com.smile.stream;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Description
 * @ClassName KeyByProcessTest
 * @Author smile
 * @date 2022.11.19 12:51
 */
public class KeyByProcessTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFunction<Event>() {
            @Override
            public void run(SourceContext<Event> ctx) throws Exception {
                ctx.collect(new Event("mimi", "/home", 1000L));

                Thread.sleep(5000L);
                ctx.collect(new Event("mimi", "/home", 11000L));

                Thread.sleep(5000L);
                ctx.collect(new Event("mimi", "/home", 11001L));
                Thread.sleep(5000L);
            }

            @Override
            public void cancel() {

            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.getTimeMillis();
            }
        }))
                .keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发：" + new Timestamp(timestamp));
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("数据到达，时间戳为：" + ctx.timestamp());
                        out.collect("数据到达，水位线为：" + ctx.timerService().currentWatermark() + "\n -------分割线	");
                        // 注册一个 10 秒后的定时器
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10 * 1000L);
                    }
                }).print();

        env.execute();
    }

    public static void testA() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new MySource())
                /**
                 * .keyBy(data -> true)是将所有数据的key 都指定为了true，其实就是所有数据拥有相同的 key，会分配到同一个分区
                 */
                .keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发时间:" + new Timestamp(timestamp));
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        out.collect("数据达到时间:" + new Timestamp(currentProcessingTime));
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 10 * 1000L);
                    }
                }).print();

        env.execute();
    }
}

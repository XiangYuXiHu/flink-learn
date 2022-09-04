package com.smile.window;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;

/**
 * @Description
 * @ClassName AggregateTest
 * @Author smile
 * @date 2022.09.04 09:49
 */
public class AggregateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new MySource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimeMillis();
                            }
                        }))
                //分组
                .keyBy(k -> true)
                // 滑动窗口 5s 步长2s
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                // 规约处理
                .aggregate(new AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double>() {

                    /**
                     * 创建累加器
                     * @return
                     */
                    @Override
                    public Tuple2<HashSet<String>, Long> createAccumulator() {
                        return Tuple2.of(new HashSet<String>(), 0L);
                    }

                    /**
                     * 数据累加，每条数据过来后执行
                     * @param value
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
                        accumulator.f0.add(value.getUser());
                        return Tuple2.of(accumulator.f0, accumulator.f1 + 1);
                    }

                    /**
                     *窗口关闭，执行此方法
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
                        return (double) accumulator.f1 / accumulator.f0.size();
                    }

                    @Override
                    public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
                        return null;
                    }
                }).print();

        env.execute("aggregated test");
    }
}

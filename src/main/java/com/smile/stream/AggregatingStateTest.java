package com.smile.stream;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 对用户点击事件流每 5 个数据统计一次平均时间戳
 *
 * @Description
 * @ClassName AggregatingStateTest
 * @Author smile
 * @date 2023.01.11 21:27
 */
public class AggregatingStateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MySource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimeMillis();
                            }
                        }))
                .keyBy(data -> data.getUser())
                .flatMap(new RichFlatMapFunction<Event, String>() {

                    /**
                     * 定义聚合状态，用来计算平均时间戳
                     */
                    AggregatingState<Event, Long> avgTsAggState;

                    /**
                     * 定义一个值状态，用来保存当前用户访问频次
                     */
                    ValueState<Long> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        avgTsAggState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>("avg-ts",
                                        new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                                            @Override
                                            public Tuple2<Long, Long> createAccumulator() {
                                                return Tuple2.of(0L, 0L);
                                            }

                                            @Override
                                            public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                                                return Tuple2.of(accumulator.f0 + value.getTimeMillis(), accumulator.f1 + 1);
                                            }

                                            @Override
                                            public Long getResult(Tuple2<Long, Long> accumulator) {
                                                return accumulator.f0 / accumulator.f1;
                                            }

                                            @Override
                                            public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                                                return null;
                                            }
                                        }, Types.TUPLE(Types.LONG, Types.LONG)));
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
                    }

                    @Override
                    public void flatMap(Event value, Collector<String> out) throws Exception {
                        Long count = countState.value();
                        if (null == count) {
                            count = 1L;
                        } else {
                            count++;
                        }
                        countState.update(count);
                        avgTsAggState.add(value);
                        if (count == 5) {
                            out.collect(value.getUser() + "平均时间戳：" + new Timestamp(avgTsAggState.get()));
                            countState.clear();
                        }
                    }
                }).print();

        env.execute();
    }
}

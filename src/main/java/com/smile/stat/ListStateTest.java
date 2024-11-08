package com.smile.stat;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @ClassName ListStateTest
 * @Author smile
 * @date 2023.01.10 20:59
 */
public class ListStateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> t, long l) {
                        return t.f2;
                    }
                })
        );
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("a", "stream-2", 3000L),
                Tuple3.of("b", "stream-2", 4000L),
                Tuple3.of("a", "stream-3", 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> t, long l) {
                        return t.f2;
                    }
                })
        );
        stream1.keyBy(t -> t.f0)
                .connect(stream2.keyBy(t -> t.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    private ListState<Tuple3<String, String, Long>> stream1ListState;
                    private ListState<Tuple3<String, String, Long>> stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stream1ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple3<String, String, Long>>("stream1-list", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                        );
                        stream2ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple3<String, String, Long>>("stream2-list", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                        );
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, Context context, Collector<String> collector) throws Exception {
                        stream1ListState.add(left);
                        stream2ListState.get().forEach(right -> collector.collect(left + " ==> " + right));
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, Context context, Collector<String> collector) throws Exception {
                        stream2ListState.add(right);
                        stream1ListState.get().forEach(left -> collector.collect(left + " => " + right));
                    }
                })
                .print();

        env.execute();
    }
}
package com.smile.stream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @ClassName BillCheckTest
 * @Author smile
 * @date 2022.11.26 16:09
 */
public class BillCheckTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //平台支付
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 1000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));

        //银行支付
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> bankStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                                return element.f3;
                            }
                        }));
        //合并两个流
        appStream.connect(bankStream)
                .keyBy(appTuple -> appTuple.f0, bankTuple -> bankTuple.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
                    //定义状态变量，保存已经到达的事件
                    private ValueState<Tuple3<String, String, Long>> appEventState;
                    private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

                    /**
                     * 状态的初始化
                     * @param parameters
                     * @throws Exception
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        appEventState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event",
                                        Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                        thirdPartyEventState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple4<String, String, String, Long>>("bank-event",
                                        Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
                        );
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来
                        if (appEventState.value() != null) {
                            out.collect("对账失败:" + appEventState.value() + "银行支付平台信息未到");
                        }
                        if (thirdPartyEventState.value() != null) {
                            out.collect("对账失败：" + thirdPartyEventState.value() + "app信息未到");
                        }
                        appEventState.clear();
                        thirdPartyEventState.clear();
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (thirdPartyEventState.value() != null) {
                            out.collect("对账成功" + value + "--" + thirdPartyEventState.value());
                            thirdPartyEventState.clear();
                        } else {
                            //更新状态
                            appEventState.update(value);
                            //注册一个 5 秒后的定时器，开始等待另一条流的事件
                            ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
                        }
                    }

                    @Override
                    public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (appEventState.value() != null) {
                            out.collect("对账成功" + value + "++" + appEventState.value());
                            appEventState.clear();
                        } else {
                            thirdPartyEventState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
                        }
                    }
                }).print();

        env.execute();
    }
}

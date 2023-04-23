package com.smile.stat;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 使用用户 id 来进行分流，然后分别统计每个用户的 pv 数据，由于我们并不想每次 pv 加一，就将统计结果发送到下游去，
 * 所以这里我们注册了一个定时器，用来隔一段时间发送 pv 的统计结果，这样对下游算子的压力不至于太大。
 * 具体实现方式是定义一个用来保存定时器时间戳的值状态变量。当定时器触发并向下游发送数据以后，
 * 便清空储存定时器时间戳的状态变量，这样当新的数据到来时，发现并没有定时器存在，就可以注册新的定时器了，
 * 注册完定时器之后将定时器的时间戳继续保存在状态变量中。
 *
 * @Description
 * @ClassName ValueStatusTest
 * @Author smile
 * @date 2023.01.09 21:38
 */
public class ValueStatusTest {

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
                .process(new KeyedProcessFunction<String, Event, String>() {

                    /**
                     * 统计状态
                     */
                    ValueState<Long> countStatus;
                    /**
                     * 时间戳状态
                     */
                    ValueState<Long> timerStatus;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        countStatus = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
                        timerStatus = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
                    }

                    @Override
                    public void processElement(Event event, Context ctx, Collector<String> out) throws Exception {
                        Long count = countStatus.value();
                        countStatus.update(null == count ? 1L : count + 1L);
                        /**
                         *  时间戳注册
                         */
                        if (null == timerStatus.value()) {
                            long ts = event.getTimeMillis() + 10 * 1000L;
                            ctx.timerService().registerEventTimeTimer(ts);
                            timerStatus.update(ts);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " pv:" + countStatus.value());
                        // 清空状态
                        timerStatus.clear();
                    }
                }).print();

        env.execute();
    }
}

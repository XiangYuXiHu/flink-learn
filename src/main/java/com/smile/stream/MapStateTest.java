package com.smile.stream;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 用映射状态来完整模拟窗口的功能
 * 计算的是每一个 url 在每一个窗口中的 pv 数据
 *
 * @Description
 * @ClassName MapStateTest
 * @Author smile
 * @date 2023.01.11 21:02
 */
public class MapStateTest {

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
                .keyBy(data -> data.getUrl())
                .process(new KeyedProcessFunction<String, Event, String>() {
                    /**
                     * 窗口长度
                     */
                    private Long windowSize = 1000L;
                    /**
                     * 用 map 保存 pv 值（窗口 start，count）
                     */
                    MapState<Long, Long> windowPvMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        windowPvMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-pv", Long.class, Long.class));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        long windowEnd = timestamp + 1;
                        Long windowStart = windowEnd - windowSize;
                        long pv = windowPvMapState.get(windowStart);
                        out.collect("url: " + ctx.getCurrentKey()
                                + " 访问量: " + pv
                                + " 窗 口 ： " + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd));
                        windowPvMapState.remove(windowStart);
                    }

                    @Override
                    public void processElement(Event event, Context ctx, Collector<String> out) throws Exception {
                        Long windowStart = event.getTimeMillis() / windowSize * windowSize;
                        long windowEnd = windowStart + windowSize;
                        // 注册 end -1 的定时器，窗口触发计算
                        ctx.timerService().registerEventTimeTimer(windowEnd - 1);
                        // 更新状态中的 pv 值
                        windowPvMapState.put(windowStart, windowPvMapState.contains(windowStart) ? windowPvMapState.get(windowStart) + 1 : 1L);
                    }
                }).print();

        env.execute();
    }
}

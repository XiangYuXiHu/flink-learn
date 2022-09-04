package com.smile.window;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 电商网站统计每小时 UV
 *
 * @Description
 * @ClassName UVCount
 * @Author smile
 * @date 2022.09.04 11:41
 */
public class UvCount {

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
                //分组
                .keyBy(key -> true)
                //滚动窗口 5s
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //处理函数
                .process(new ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {

                    @Override
                    public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        HashSet<String> set = new HashSet<>();
                        for (Event event : elements) {
                            set.add(event.getUser());
                        }

                        //结合窗口信息，包装输出内容
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        out.collect("窗口：" + new Timestamp(start) + "~" + new Timestamp(end)
                                + "访客数量是:" + set.size());

                    }
                }).print();

        env.execute("UvCount");
    }
}

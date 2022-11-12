package com.smile.window;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 延迟数据
 *
 * @Description
 * @ClassName LateDataTest
 * @Author smile
 * @date 2022.11.12 20:03
 */
public class LateDataTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new MySource())
                /**
                 * 水位线设置延迟
                 */
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                        Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTimeMillis())
                ).keyBy(data -> data.getUrl())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                /**
                 * 窗口处理延迟1分钟
                 */
                .allowedLateness(Time.minutes(1))
                /**
                 * 测输出流标签
                 */
                .sideOutputLateData(new OutputTag<Event>("lateData") {
                })
                .aggregate(
                        new AggregateFunction<Event, Long, Long>() {
                            /**
                             * 创建累加器
                             *
                             * @return
                             */
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            /**
                             * 数据执行
                             *
                             * @param value
                             * @param accumulator
                             * @return
                             */
                            @Override
                            public Long add(Event value, Long accumulator) {
                                return accumulator + 1;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        },
                        /**
                         * 自定义窗口函数
                         */
                        new ProcessWindowFunction<Long, Object, String, TimeWindow>() {

                            @Override
                            public void process(String url, Context context, Iterable<Long> elements, Collector<Object> out) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();

                                out.collect("url=" + url + ",count=" + elements.iterator().next()
                                        + ",windowStart=" + new Timestamp(start) + ",windowEnd=" + new Timestamp(end));
                            }
                        }
                ).print();

        env.execute();
    }
}

package com.smile.window;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 在网站的各种统计指标中，一个很重要的统计指标就是热门的链接；想要得到热门的 url，
 * 前提是得到每个链接的“热门度”。一般情况下，可以用
 * url 的浏览量（点击量）表示热门度。我们这里统计 10 秒钟的 url 浏览量，
 * 每 5 秒钟更新一次； 另外为了更加清晰地展示，还应该把窗口的起始结束时间一起输出。
 *
 * @Description
 * @ClassName UrlCountByWindowTest
 * @Author smile
 * @date 2022.09.04 12:09
 */
public class UrlCountByWindowTest {

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
                .keyBy(key -> key.getUrl())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //传入增量聚合函数与全窗口函数
                .aggregate(new AggregateFunction<Event, Long, Long>() {
                    /**
                     * 创建累加器
                     *
                     * @return
                     */
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

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
                }, new ProcessWindowFunction<Long, Object, String, TimeWindow>() {
                    @Override
                    public void process(String url, Context context, Iterable<Long> elements, Collector<Object> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        out.collect("url=" + url + ",count=" + elements.iterator().next() +
                                ",start=" + new Timestamp(start) + ",end=" + new Timestamp(end));
                    }
                }).print();

        env.execute("UrlCountByWindowTest");
    }
}

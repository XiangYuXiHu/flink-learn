package com.smile.window;

import com.smile.domain.Event;
import com.smile.domain.UrlViewCount;
import com.smile.source.MySource;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * 在网站的各种统计指标中，一个很重要的统计指标就是热门的链接；想要得到热门的 url，
 * 前提是得到每个链接的“热门度”。一般情况下，可以用
 * url 的浏览量（点击量）表示热门度。我们这里统计 10 秒钟的 url 浏览量，
 * 每 5 秒钟更新一次； 另外为了更加清晰地展示，还应该把窗口的起始结束时间一起输出。
 * <p>
 * 一是对数据进行按键分区，分别统计浏览量；二是进行增量聚合，得到结果最后再做排序输出
 * <p>
 * 先按照 url 对数据进行 keyBy 分区，然后开窗进行增量聚合。这里就会发现一个问题：我们进行按键分区之后，
 * 窗口的计算就会只针对当前 key 有效了；也就是说， 每个窗口的统计结果中，只会有一个 url 的浏览量，
 * 这是无法直接用 ProcessWindowFunction 进行排序的。所以我们只能分成两步：先对每个url 链接统计出浏览量，
 * 然后再将统计结果收集起来，排序输出最终结果。
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
                //利用增量聚合函数的特性，每来一条数据就更新一次对应 url 的浏览量
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
                }, new ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {
                    @Override
                    public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
                    }
                })
                // 对结果中同一个窗口的统计数据，进行排序处理
                .keyBy(data -> data.getEnd())
                //排序获取TopN
                .process(new KeyedProcessFunction<Long, UrlViewCount, String>() {

                    private ListState<UrlViewCount> urlViewCountListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        urlViewCountListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<UrlViewCount>("url-view-count-list", Types.toClass(UrlViewCount.class))
                        );
                    }

                    @Override
                    public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
                        urlViewCountListState.add(value);
                        // 注册 window end + 1ms 后的定时器，等待所有数据到齐开始排序
                        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 将数据从列表状态变量中取出，放入 ArrayList，方便排序
                        ArrayList<UrlViewCount> urlViewCountList = new ArrayList<>();
                        urlViewCountListState.get().forEach(urlViewCountList::add);

                        urlViewCountListState.clear();

                        String result = urlViewCountList.stream().sorted((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue())
                                .limit(5)
                                .map(t2 -> "url:" + t2.getUrl() + "浏览量：" + t2.getCount())
                                .collect(Collectors.joining("\n"));

                        out.collect(result + "\n=================================");
                    }
                }).print()
        ;

        env.execute("UrlCountByWindowTest");
    }
}

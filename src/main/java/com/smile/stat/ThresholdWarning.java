package com.smile.stat;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @ClassName ThresholdWarning
 * @Author smile
 * @date 2023.04.09 10:03
 */
public class ThresholdWarning extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, List<Long>>> {

    private ListState<Long> abnormalList;

    private Long threshold;

    private Integer numberOfTimes;

    public ThresholdWarning(Long threshold, Integer numberOfTimes) {
        this.threshold = threshold;
        this.numberOfTimes = numberOfTimes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        /**
         * 通过状态名称(句柄)获取状态实例，如果不存在则会自动创建
         */
        abnormalList = getRuntimeContext().getListState(new ListStateDescriptor("abnormalList", Long.class));
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, List<Long>>> out) throws Exception {
        Long val = value.f1;
        /**
         * 如果输入值超过阈值，则记录该次不正常的数据信息
         */
        if (val >= threshold) {
            abnormalList.add(val);
        }

        ArrayList<Long> list = Lists.newArrayList(abnormalList.get().iterator());
        /**
         * 如果不正常的数据出现达到一定次数，则输出报警信息
         */
        if (list.size() > numberOfTimes) {
            out.collect(Tuple2.of(value.f0 + "触发报警", list));
            /**
             * 报警信息输出后，清空状态
             */
            abnormalList.clear();
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = env.fromElements(
                Tuple2.of("a", 50L), Tuple2.of("a", 80L), Tuple2.of("a", 400L),
                Tuple2.of("a", 100L), Tuple2.of("a", 200L), Tuple2.of("a", 200L),
                Tuple2.of("b", 100L), Tuple2.of("b", 200L), Tuple2.of("b", 200L),
                Tuple2.of("b", 500L), Tuple2.of("b", 600L), Tuple2.of("b", 700L));
        tuple2DataStreamSource
                .keyBy(e -> e.f0)
                .flatMap(new ThresholdWarning(200L, 3))
                .printToErr();
        env.execute("Managed Keyed State");
    }
}

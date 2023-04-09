package com.smile.stat;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @ClassName ThresholdWarning2
 * @Author smile
 * @date 2023.04.09 11:39
 */
public class ThresholdWarning2 extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, List<Tuple2<String, Long>>>>
        implements CheckpointedFunction {
    private List<Tuple2<String, Long>> bufferedData;
    private transient ListState<Tuple2<String, Long>> checkpointState;
    private Long threshold;
    private Integer numberOfTimes;

    public ThresholdWarning2(Long threshold, Integer numberOfTimes) {
        this.bufferedData = new ArrayList<>();
        this.threshold = threshold;
        this.numberOfTimes = numberOfTimes;
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, List<Tuple2<String, Long>>>> out) throws Exception {
        Long val = value.f1;
        if (val >= threshold) {
            bufferedData.add(value);
        }

        /**
         * 超过指定次数则输出报警信息
         */
        if (bufferedData.size() > numberOfTimes) {
            /**
             * 顺便输出状态实例的hashcode
             */
            out.collect(Tuple2.of(checkpointState.hashCode() + "告警", bufferedData));
            bufferedData.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        /**
         * 在进行快照时，将数据存储到checkPointedState
         */
        checkpointState.clear();
        for (Tuple2<String, Long> e : bufferedData) {
            checkpointState.add(e);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        checkpointState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<Tuple2<String, Long>>("abnormalData",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        })));
        /**
         * 如果发生重启，则需要从快照中将状态进行恢复
         */
        if (context.isRestored()) {
            for (Tuple2<String, Long> element : checkpointState.get()) {
                bufferedData.add(element);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 开启检查点机制
        env.enableCheckpointing(1000);
// 设置并行度为1
        DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = env.setParallelism(3).fromElements(
                Tuple2.of("a", 50L), Tuple2.of("a", 80L), Tuple2.of("a", 400L),
                Tuple2.of("a", 100L), Tuple2.of("a", 200L), Tuple2.of("a", 200L),
                Tuple2.of("b", 100L), Tuple2.of("b", 200L), Tuple2.of("b", 200L),
                Tuple2.of("b", 500L), Tuple2.of("b", 600L), Tuple2.of("b", 700L));
        tuple2DataStreamSource
                .flatMap(new ThresholdWarning2(100L, 3))
                .printToErr();
        env.execute("Managed Keyed State");
    }
}

package com.smile.sink;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @ClassName BufferSinkTest
 * @Author smile
 * @date 2023.04.29 14:09
 */
public class BufferSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MySource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimeMillis();
                            }
                        })).addSink(new BufferingSink(10));
        env.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        private final int threshold;
        private transient ListState<Event> checkpointState;
        private List<Event> bufferElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferElements.add(value);
            if (bufferElements.size() == threshold) {
                for (Event event : bufferElements) {
                    System.out.println("---" + event);
                }
                System.out.println("-----------完成-------");
                bufferElements.clear();
            }

        }

        /**
         * 把当前局部变量中的所有元素写入到检查点中
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointState.clear();
            for (Event event : bufferElements) {
                checkpointState.add(event);
            }
        }

        /**
         * 初始化状态时调用这个方法，也会在恢复状态时调用
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buff-element",
                    Types.POJO(Event.class));
            checkpointState = context.getOperatorStateStore().getListState(descriptor);
            /**
             * 如果是从故障中恢复，就将 ListState 中的所有元素添加到局部变量中
             */
            if (context.isRestored()) {
                for (Event event : checkpointState.get()) {
                    bufferElements.add(event);
                }
            }
        }
    }
}

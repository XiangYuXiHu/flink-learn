package com.smile.stat;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @ClassName BroadcastStatTest
 * @Author smile
 * @date 2023.05.01 13:49
 */
public class BroadcastStatTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Action> actionStream = env.fromElements(new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy")
        );
        // 定义行为模式流，代表了要检测的标准
        DataStreamSource<Pattern> patternStream = env.fromElements(
                        new Pattern("login", "pay"),
                        new Pattern("login", "buy")
                );
        // 定义广播状态的描述器，创建广播流
        MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcast = patternStream.broadcast(bcStateDescriptor);
        // 将事件流和广播流连接起来，进行处理
        actionStream.keyBy(data -> data.userId)
                .connect(broadcast)
                .process(new KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>() {
                    // 定义一个值状态，保存上一次用户行为
                    ValueState<String> prevActionState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAction", String.class));
                    }

                    @Override
                    public void processElement(Action action, ReadOnlyContext readOnlyContext, Collector<Tuple2<String, Pattern>> collector) throws Exception {
                        // 数据流，只拥有配置流读取的权力
                        Pattern patterns = readOnlyContext.getBroadcastState(new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class))).get(null);
                        String prevAction = prevActionState.value();
                        if (null != patterns && null != prevAction) {
                            // 如果前后两次行为都符合模式定义，输出一组匹配
                            // 比如：Alice行为为login，而且动作为pay则符合条件
                            if (patterns.action1.equals(prevAction) && patterns.action2.equals(action.action)) {
                                collector.collect(new Tuple2<>(readOnlyContext.getCurrentKey(), patterns));
                            }
                        }
                        // 更新状态
                        prevActionState.update(action.action);
                    }

                    @Override
                    public void processBroadcastElement(Pattern pattern, Context context, Collector<Tuple2<String, Pattern>> collector) throws Exception {
                        // 配置流，拥有上下文修改的权利
                        BroadcastState<Void, Pattern> bcState = context.getBroadcastState(new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class)));
                        // 将广播状态更新为当前的 pattern
                        bcState.put(null, pattern);
                    }
                }).setParallelism(2)
                .print();
        env.execute();
    }

    public static class Action {
        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId=" + userId +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}

package com.smile.stream;

import com.smile.domain.UserAction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * @Description
 * @ClassName ReduceOperator
 * @Author smile
 * @date 2022.08.13 14:43
 */
public class ReduceOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<UserAction> source = environment.fromCollection(Arrays.asList(
                new UserAction("user1", 1293984000, "click", "苹果", BigDecimal.valueOf(30)),
                new UserAction("user2", 1293984001, "browse", "西瓜", BigDecimal.valueOf(12)),
                new UserAction("user2", 1293984001, "browse11", "水蜜桃", BigDecimal.valueOf(25)),
                new UserAction("user2", 1293984003, "browse", "香蕉", BigDecimal.valueOf(12)),
                new UserAction("user1", 1293984000, "click11", "桃子", BigDecimal.valueOf(40)),
                new UserAction("user1", 1293984000, "click", "葡萄", BigDecimal.valueOf(12)),
                new UserAction("user1", 1293984000, "click", "甜瓜", BigDecimal.valueOf(12))
        ));

        reduceOperate(source);
        environment.execute("reduce");
    }

    public static void reduceOperate(DataStreamSource<UserAction> source) {
        source.keyBy(UserAction::getUserId)
                .reduce(new ReduceFunction<UserAction>() {
                    @Override
                    public UserAction reduce(UserAction value1, UserAction value2) throws Exception {
                        value1.setPrice(value1.getPrice().add(value2.getPrice()));
                        return value1;
                    }
                }).returns(UserAction.class)
                .print();
    }
}

package com.smile.stream;

import com.smile.domain.UserAction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

/**
 * @Description
 * @ClassName KeyByOperator
 * @Author smile
 * @date 2022.08.13 14:17
 */
public class KeyByOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<UserAction> dataStream = environment.fromElements(
                new UserAction("jim", 10001, "click", "phone", BigDecimal.valueOf(10)),
                new UserAction("jack", 10002, "browse", "pc", BigDecimal.valueOf(11)),
                new UserAction("luck", 10003, "click", "mac", BigDecimal.valueOf(12)),
                new UserAction("tony", 10004, "click", "phone", BigDecimal.valueOf(13)));

        KeyedStream<UserAction, String> keyedStream = dataStream.keyBy(new KeySelector<UserAction, String>() {
            @Override
            public String getKey(UserAction value) throws Exception {
                return value.getAction();
            }
        });

        keyedStream.sum("userNo").print();
        keyedStream.maxBy("userNo").print();

        environment.execute("keyBy");
    }
}

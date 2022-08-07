package com.smile.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * socket 读取数据
 *
 * @Description
 * @ClassName SocketStream
 * @Author smile
 * @date 2022.08.07 20:31
 */
public class SocketStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("192.168.21.128", 9999);
        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                if (null != tokens) {
                    Arrays.stream(tokens).map(token -> new Tuple2(token, 1)).forEach(out::collect);
                }
            }
        }).keyBy(value -> value.f0).sum(1).print();

        environment.execute("socket stream");
    }
}

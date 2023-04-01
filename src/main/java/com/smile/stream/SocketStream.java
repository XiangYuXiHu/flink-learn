package com.smile.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
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
        socketStream(environment);
    }

    public static void socketStream(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);
        socketTextStream.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] split = StringUtils.split(value, ",");
            if (null != split) {
                Arrays.stream(split).forEach(out::collect);
            }
        }).returns(Types.STRING)
                .map((MapFunction<String, Tuple2<String, Integer>>) value -> new Tuple2<>(value, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(e -> e.f0).sum(1).print();
        env.execute("socket stream");
    }
}

package com.smile.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Description
 * @ClassName CoMapTest
 * @Author smile
 * @date 2022.11.26 15:44
 */
public class CoMapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> tuple2DataStreamSource = env.fromElements(new Tuple2<>("a", 2), new Tuple2<>("b", 5));
        DataStreamSource<Integer> dataStreamSource = env.fromElements(2, 3, 6);
        ConnectedStreams<Tuple2<String, Integer>, Integer> connect = tuple2DataStreamSource.connect(dataStreamSource);
        connect.map(new CoMapFunction<Tuple2<String, Integer>, Integer, Integer>() {
            @Override
            public Integer map1(Tuple2<String, Integer> value) throws Exception {
                return value.f1;
            }

            @Override
            public Integer map2(Integer value) throws Exception {
                return value;
            }
        }).map(x -> x * 100).print();

        env.execute();
    }
}

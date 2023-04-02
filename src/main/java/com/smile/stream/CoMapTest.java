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

        coMapStream(env);
    }

    public static void coMapStream(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Tuple2<String, Integer>> stream2 = env.fromElements(Tuple2.of("jim", 4), Tuple2.of("lucy", 5), Tuple2.of("lily", 6));
        stream1.connect(stream2).map(new CoMapFunction<Integer, Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer map1(Integer value) throws Exception {
                return value;
            }

            @Override
            public Integer map2(Tuple2<String, Integer> value) throws Exception {
                return value.f1;
            }
        }).map(e -> e * 100).print();

        env.execute("co map stream");
    }
}

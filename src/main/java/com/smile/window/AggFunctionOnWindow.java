package com.smile.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @ClassName AggFunctionOnWindow
 * @Author smile
 * @date 2023.04.11 22:10
 */
public class AggFunctionOnWindow {

    public static final Tuple3[] MATH = new Tuple3[]{
            Tuple3.of("class1", "jack", 100),
            Tuple3.of("class1", "Lucy", 80),
            Tuple3.of("class1", "jim", 90),
            Tuple3.of("class1", "tom", 110),
            Tuple3.of("class2", "wang", 90),
            Tuple3.of("class2", "li", 80),
            Tuple3.of("class2", "li2", 80),
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, String, Integer>> tuple3DataStreamSource = env.fromElements(MATH);
        SingleOutputStreamOperator<Double> avgScore = tuple3DataStreamSource.keyBy(e -> e.f0).countWindow(3).aggregate(new AverageAggrate());
        avgScore.print();
        env.execute();
    }

    public static class AverageAggrate implements AggregateFunction<Tuple3<String, String, Integer>, Tuple2<Integer, Integer>, Double> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return new Tuple2<>(0, 0);
        }

        @Override
        public Tuple2<Integer, Integer> add(Tuple3<String, String, Integer> value, Tuple2<Integer, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f2, accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return Double.valueOf(accumulator.f0 / accumulator.f1);
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}

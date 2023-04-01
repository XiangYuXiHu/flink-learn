package com.smile.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 统计单词数量
 *
 * @Description
 * @ClassName WordCount
 * @Author smile
 * @date 2022.07.09 17:05
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        wordCount(env);
    }

    public static void wordCount(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/hello.txt");

        stream.flatMap((FlatMapFunction<String, String>) (value, out) -> Arrays.stream(value.split("\t")).forEach(out::collect))
                .returns(Types.STRING)
                .filter((FilterFunction<String>) value -> StringUtils.isNoneBlank(value))
                .map((MapFunction<String, Tuple2>) value -> Tuple2.of(value, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(e -> e.f0)
                .sum(1)
                .print();

        env.execute("word count");
    }

}

package com.smile.stream;

import com.smile.source.MyParallelSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义数据源
 *
 * @Description
 * @ClassName MySourceFunction
 * @Author smile
 * @date 2022.08.08 22:33
 */
public class MySourceFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        parallelStream(environment);
    }

    public static void parallelStream(StreamExecutionEnvironment env) throws Exception {
        env.addSource(new MyParallelSource()).setParallelism(2).print();
        env.execute("parallel stream");
    }
}

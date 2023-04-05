package com.smile.stream;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @ClassName RichMapFunctionOperator
 * @Author smile
 * @date 2022.08.13 16:06
 */
public class RichMapFunctionOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        richMapFunction(environment);
    }

    public static void richMapFunction(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> streamSource = env.fromElements("hadoop", "java", "flink", "hive");

        streamSource.map(new RichMapFunction<String, String>() {

            private LongCounter counter = new LongCounter(0);

            /**
             * 默认生命周期方法：初始化方法，在每个并行度上，只会调用一次
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open方法...");
                super.open(parameters);
                getRuntimeContext().addAccumulator("counter", counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }

            @Override
            public void close() throws Exception {
                System.out.println("close方法执行一次...");
                super.close();
            }
        }).setParallelism(2);

        JobExecutionResult execute = env.execute("execute");
        Object result = execute.getAccumulatorResult("counter");
        System.out.println(result);
    }
}

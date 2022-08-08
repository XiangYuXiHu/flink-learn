package com.smile.stream;

import com.smile.domain.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
        DataStreamSource<Event> source = environment.addSource(new MyParallelSource()).setParallelism(2);
        source.print();

        environment.execute("time");
    }
}

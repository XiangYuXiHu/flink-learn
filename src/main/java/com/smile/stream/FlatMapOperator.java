package com.smile.stream;

import com.smile.domain.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @ClassName FlatMapOperator
 * @Author smile
 * @date 2022.08.13 08:20
 */
public class FlatMapOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> eventStream = environment.fromElements(new Event("jack", "http://www.bd.com", 1000L),
                new Event("jim", "http://abc.com", 1001L));

        eventStream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                out.collect(value.getUser());
                out.collect(value.getUrl());
                out.collect(value.getTimeMillis().toString());
            }
        }).print();

        environment.execute("flatMap");
    }
}

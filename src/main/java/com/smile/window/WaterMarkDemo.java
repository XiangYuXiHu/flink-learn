package com.smile.window;

import com.smile.domain.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @Description
 * @ClassName WaterMarkDemo
 * @Author smile
 * @date 2022.08.28 11:21
 */
public class WaterMarkDemo {

    public static void main(String[] args) throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<Event> dataStream = env.fromElements(new Event("mary", "/home", 1200L),
                new Event("jim", "/game", 1300L),
                new Event("jack", "/index", 1400L));

        /**
         * 乱序流中需要等待迟到数据到齐，所以必须设置一个固定量的延迟时间（ Fixed Amount of Lateness）
         * 生成水位线的时间戳，就是当前数据流中最大的时间戳减去延迟的结果，相当于把表调慢，当前时钟会滞后于数据的最大时间戳。
         * 调用 WatermarkStrategy. forBoundedOutOfOrderness()方法就可以实现
         */
        dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long recordTimestamp) {
                        return event.getTimeMillis();
                    }
                }));
        env.execute("waterMark");
    }
}

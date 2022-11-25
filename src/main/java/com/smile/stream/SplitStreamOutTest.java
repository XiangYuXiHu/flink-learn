package com.smile.stream;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Description
 * @ClassName SplitStreamOutTest
 * @Author smile
 * @date 2022.11.25 21:50
 */
public class SplitStreamOutTest {

    public static void main(String[] args) throws Exception {
        /**
         * 定义输出标签，输出数据类型为三元组（user,url,timestamp）
         */
        OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("mary") {
        };
        OutputTag<Tuple3<String, String, Long>> sliceTag = new OutputTag<Tuple3<String, String, Long>>("slice") {
        };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> process = env.addSource(new MySource())
                .process(new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
                        if (event.getUser().equalsIgnoreCase("mary")) {
                            ctx.output(maryTag, new Tuple3<>(event.getUser(), event.getUrl(), event.getTimeMillis()));
                        } else if (event.getUser().equalsIgnoreCase("slice")) {
                            ctx.output(sliceTag, new Tuple3<>(event.getUser(), event.getUrl(), event.getTimeMillis()));
                        } else {
                            out.collect(event);
                        }
                    }
                });

        process.getSideOutput(maryTag).print();
        process.getSideOutput(sliceTag).print();
        process.print();

        env.execute("split");
    }
}

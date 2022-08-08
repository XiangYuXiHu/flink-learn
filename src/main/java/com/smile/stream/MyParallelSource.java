package com.smile.stream;

import com.smile.domain.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 并行数据源
 *
 * @Description
 * @ClassName MySource
 * @Author smile
 * @date 2022.08.08 22:17
 */
class MyParallelSource implements ParallelSourceFunction<Event> {

    /**
     * 控制数据生成标识
     */
    private Boolean running = true;

    private String[] users = {"mary", "alice", "bob", "cary"};

    private String[] urls = {"./home", "./fav", "./cat", "./prod?id=1"};

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        while (running) {
            ctx.collect(new Event(users[random.nextInt(users.length)], urls[random.nextInt(urls.length)], Calendar.getInstance().getTimeInMillis()));
        }
        TimeUnit.SECONDS.sleep(1);
    }

    @Override
    public void cancel() {
        running = false;
    }
}

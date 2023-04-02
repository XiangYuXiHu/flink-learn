package com.smile.source;

import com.smile.domain.Event;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 并行数据源
 *
 * @Description
 * @ClassName MyParallelSource
 * @Author smile
 * @date 2023.04.02 07:09
 */
public class MyParallelSource implements ParallelSourceFunction<Event> {

    /**
     * 控制数据生成标识
     */
    private Boolean running = true;

    private String[] users = {"mary", "alice", "bob", "cary"};

    private String[] urls = {"./home", "./fav", "./cat", "./prod?id=1"};

    /**
     * 使用运行时上下文对象（SourceContext）向下游发送数据
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        while (running) {
            Random random = new Random();
            String userName = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            ctx.collect(new Event(userName, url, Calendar.getInstance().getTimeInMillis()));
            TimeUnit.SECONDS.sleep(1);
        }
    }

    /**
     * 过标识位控制退出循环，来达到中断数据源的效果
     */
    @Override
    public void cancel() {
        running = false;
    }
}

package com.smile.source;

import com.smile.domain.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @ClassName MyRedisSource
 * @Author smile
 * @date 2022.08.14 10:01
 */
public class MySource implements SourceFunction<Event> {

    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();

        String[] users = new String[]{"mary", "slice", "lucy", "smile"};
        String[] urls = new String[]{"./hello", "./game", "./hai", "./name"};
        while (isRunning) {
            ctx.collect(new Event(users[random.nextInt(users.length)], urls[random.nextInt(urls.length)], System.currentTimeMillis()));

            TimeUnit.SECONDS.sleep(3);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

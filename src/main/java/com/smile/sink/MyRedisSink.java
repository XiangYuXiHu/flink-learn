package com.smile.sink;

import com.smile.domain.Event;
import com.smile.source.MySource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Description
 * @ClassName RedisSink
 * @Author smile
 * @date 2022.08.14 09:58
 */
public class MyRedisSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();
        environment.addSource(new MySource()).addSink(new RedisSink<>(jedisPoolConfig, new MyRedisMapper()));

        environment.execute("redis sink");
    }

    public static class MyRedisMapper implements RedisMapper<Event> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }

        @Override
        public String getKeyFromData(Event event) {
            return event.getUser();
        }

        @Override
        public String getValueFromData(Event event) {
            return event.getUrl();
        }
    }
}

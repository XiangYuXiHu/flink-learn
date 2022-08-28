package com.smile.sink;

import com.smile.domain.User;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * * link 为此专门提供了一个流式文件系统的连接器：StreamingFileSink，它继承自抽象类RichSinkFunction，
 * * 而且集成了 Flink 的检查点（checkpoint）机制，用来保证精确一次（exactly once）的一致性语义。
 *
 * @Description
 * @ClassName SinkFile
 * @Author smile
 * @date 2022.08.14 08:32
 */
public class SinkFile {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<User> stream = env.fromElements(new User("1001", "小明"),
                new User("1001", "小王"),
                new User("1003", "小李"), new User("1004", "小美"),
                new User("1005", "小刚"),
                new User("1005", "笑笑"));

        DefaultRollingPolicy<User, String> rollPolicy = DefaultRollingPolicy.builder()
                /**
                 * 最近 2 分钟没有收到新的数据,产生分区文件
                 */
                .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))
                /**
                 *  最近 5 分钟没有收到新的数据，产生分区文件
                 */
                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                /**
                 * 文件大小已达到 128M ,产生分区文件
                 */
                .withMaxPartSize(128 * 1024 * 1024).build();

        OutputFileConfig config = OutputFileConfig.builder().withPartPrefix("prefix").withPartSuffix(".txt").build();
        StreamingFileSink<User> fileSink = StreamingFileSink
                .forRowFormat(new Path("./output"), new SimpleStringEncoder<User>("utf-8"))
                /**采用默认的分桶策略DateTimeBucketAssigner，它基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH*/
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .withRollingPolicy(rollPolicy)
                .withBucketCheckInterval(1)
                .withOutputFileConfig(config).build();

        stream.keyBy(key -> key.getUserId()).addSink(fileSink);

        env.execute("streaming file");

    }
}

package com.smile.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @Description
 * @ClassName KafkaSink
 * @Author smile
 * @date 2022.08.14 09:16
 */
public class KafkaSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        kafkaSink(environment);
    }

    public static void kafkaSink(StreamExecutionEnvironment environment) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.21.120:9092");
        DataStreamSource<String> streamSource = environment.addSource(
                new FlinkKafkaConsumer<String>("fk-stream-in-topic", new SimpleStringSchema(), properties));

        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer("fk-stream-out-topic", new KafkaSerializationSchema<String>() {

            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>("fk-stream-out-topic", element.getBytes());
            }
        }, properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE, 5);

        streamSource.map((MapFunction<String, String>) value -> value + value).addSink(flinkKafkaProducer);

        environment.execute("kafkaSink");
    }
}

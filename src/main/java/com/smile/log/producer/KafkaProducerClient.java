package com.smile.log.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @ClassName KafkaProducerClient
 * @Author smile
 * @date 2023.04.15 22:12
 */
public class KafkaProducerClient {

    private static Random random = new Random(47);
    private static String[] LEVEL = new String[]{"L", "H"};
    private static String[] DOMAIN = new String[]{
            "www.baidu.com", "www.sina.com", "www.long.cn",
            "www.google.cn", "www.tencent.cn", "www.mi.cn"
    };
    private static String TOPIC = "my-topic";

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.21.120:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        while (true) {
            StringBuilder builder = new StringBuilder();
            String message = builder.append("computeStream").append("\t")
                    .append("CN").append("\t").append(getLevel()).append("\t")
                    .append(getCurrentTime()).append("\t")
                    .append(getDomain()).append("\t")
                    .append(getTraffic()).toString();
            kafkaProducer.send(new ProducerRecord<>(TOPIC, message));
            TimeUnit.SECONDS.sleep(1);
            System.out.println("发送消息成功:" + message);
        }
    }

    /**
     * 获取等级
     *
     * @return
     */
    private static String getLevel() {
        int index = random.nextInt(LEVEL.length);
        return LEVEL[index];
    }

    /**
     * 获取当前时间
     *
     * @return
     */
    private static String getCurrentTime() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return formatter.format(LocalDateTime.now());
    }

    /**
     * 获取域名
     *
     * @return
     */
    private static String getDomain() {
        int index = random.nextInt(DOMAIN.length);
        return DOMAIN[index];
    }

    /**
     * 流量
     *
     * @return
     */
    private static int getTraffic() {
        return random.nextInt(10000);
    }

}

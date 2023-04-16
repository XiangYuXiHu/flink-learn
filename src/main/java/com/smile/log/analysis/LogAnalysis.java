package com.smile.log.analysis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

/**
 * @Description
 * @ClassName LogAnalysis
 * @Author smile
 * @date 2023.04.15 22:56
 */
public class LogAnalysis {

    private static String TOPIC = "my-topic";

    public static FlinkKafkaConsumer<String> logStream() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.21.120:9092");
        properties.setProperty("group.id", "log-traffic");
        return new FlinkKafkaConsumer<String>(TOPIC, new SimpleStringSchema(), properties);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<String> kafkaConsumer = logStream();
        DataStreamSource<String> dataSource = env.addSource(kafkaConsumer);
        dataSource.map(log -> {
            String[] splits = log.split("\t");
            String level = splits[2];
            String time = splits[3];
            Long seconds = getCurrentTime(time);
            String domain = splits[4];
            String traffic = splits[5];

            Cell cell = new Cell();
            cell.setLevel(level);
            cell.setTime(seconds);
            cell.setDomain(domain);
            cell.setTraffic(traffic);
            return cell;
        }).filter(e -> e.getLevel().equalsIgnoreCase("H"))
                .map(new MapFunction<Cell, Tuple3<String, Long, Integer>>() {

                    @Override
                    public Tuple3<String, Long, Integer> map(Cell cell) throws Exception {
                        String domain = cell.getDomain();
                        Long time = cell.getTime();
                        String traffic = cell.getTraffic();
                        return Tuple3.of(domain, time, Integer.valueOf(traffic));
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {
                    Long maxOutOfOrder = 10000L;
                    Long currentMaxTimestamp = 0L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrder);
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element, long recordTimestamp) {
                        Long time = element.f1;
                        currentMaxTimestamp = Math.max(time, currentMaxTimestamp);
                        return currentMaxTimestamp;
                    }
                }).keyBy(e -> e.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple3<String, Long, Integer>, Elem, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple3<String, Long, Integer>> elements, Collector<Elem> out) throws Exception {
                        System.out.println("----------------" + elements);
                        int sum = 0;
                        long maxTime = 0;
                        String domain = "";
                        Iterator<Tuple3<String, Long, Integer>> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            Tuple3<String, Long, Integer> cell = iterator.next();
                            domain = cell.f0;
                            sum += cell.f2;
                            Long time = cell.f1;
                            maxTime = maxTime > time ? maxTime : time;
                        }
                        String currentTime = getCurrentTime(maxTime);
                        Elem elem = new Elem();
                        elem.setDomain(domain);
                        elem.setTime(currentTime);
                        elem.setTraffic(sum);
                        out.collect(elem);
                    }
                }).print();
        env.execute("logAnalysis");
    }

    /**
     * 时间s
     *
     * @param time
     * @return
     */
    private static Long getCurrentTime(String time) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return simpleDateFormat.parse(time).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * 转为时间
     *
     * @param time
     * @return
     */
    private static String getCurrentTime(Long time) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(time));
    }
}

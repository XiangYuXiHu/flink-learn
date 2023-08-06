package com.smile.table;

import com.smile.domain.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description
 * @ClassName TableToDataStream
 * @Author smile
 * @date 2023.06.04 07:10
 */
public class TableToDataStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> source = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L));
        /**
         * 获取表环境
         */
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        /**
         * 通过流，stream往上转换到table表结构层
         */
        Table table = streamTableEnv.fromDataStream(source);
        Table result = streamTableEnv.sqlQuery("select user,timeMillis from " + table);
        streamTableEnv.toDataStream(result).print("sql->");

        env.execute("tableToDataStream");
    }
}

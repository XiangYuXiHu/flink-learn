package com.smile.table;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * @Description
 * @ClassName RowTest
 * @Author smile
 * @date 2024.10.17 21:49
 */
public class RowTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);

        DataStreamSource<Row> source = environment.fromElements(Row.ofKind(RowKind.INSERT, "张三", 20),
                Row.ofKind(RowKind.INSERT, "李四", 18),
                Row.ofKind(RowKind.UPDATE_BEFORE, "yy", 30),
                Row.ofKind(RowKind.UPDATE_AFTER, "qq", 123));

        Table table = tableEnv.fromChangelogStream(source);
        table.execute().print();
    }
}

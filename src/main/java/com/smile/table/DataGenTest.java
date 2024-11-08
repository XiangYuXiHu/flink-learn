package com.smile.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description
 * @ClassName DataGenTest
 * @Author smile
 * @date 2024.10.20 16:37
 */
public class DataGenTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        String sql="CREATE TABLE datagen (" +
                "f_sequence INT," +
                "f_random INT," +
                "f_random_str STRING," +
                "ts AS localtimestamp," +
                "WATERMARK FOR ts AS ts" +
                ") with(" +
                "'connector'='datagen'," +
                "'rows-per-second'='5'," +
                "'fields.f_sequence.kind'='sequence'," +
                "'fields.f_sequence.start'='1'," +
                "'fields.f_sequence.end'='1000'," +
                "'fields.f_random.min'='1'," +
                "'fields.f_random.max'='1000'," +
                "'fields.f_random_str.length'='10')";
        tableEnvironment.executeSql(sql);
        tableEnvironment.sqlQuery("select * from datagen").execute().print();
    }
}

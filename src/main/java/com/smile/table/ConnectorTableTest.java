package com.smile.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description
 * @ClassName ConnectorTableTest
 * @Author smile
 * @date 2024.10.20 15:40
 */
public class ConnectorTableTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);
        String sql = "create table t_member_copy(" +
                "id int," +
                "username string," +
                "password string," +
                "interests string)" +
                "with (" +
                "'connector'='jdbc'," +
                "'url'='jdbc:mysql://localhost:3306/mybatis?serverTimeZone=Asia/Shanghai'," +
                "'table-name'='t_member'," +
                "'driver'='com.mysql.jdbc.Driver'," +
                "'username'='admin'," +
                "'password'='admin')";
        tableEnv.executeSql(sql);
        tableEnv.sqlQuery("select * from t_member_copy").execute().print();
    }
}

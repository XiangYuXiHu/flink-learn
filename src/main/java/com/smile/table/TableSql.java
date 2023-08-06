package com.smile.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Description
 * @ClassName TableSql
 * @Author smile
 * @date 2023.06.03 08:12
 */
public class TableSql {

    public static void main(String[] args) {
        EnvironmentSettings conf = EnvironmentSettings.newInstance().inStreamingMode().build();

        TableEnvironment tableEnvironment = TableEnvironment.create(conf);
        tableEnvironment.executeSql("create table event_table (" +
                " user_name STRING, url STRING) with ('connector'='filesystem'," +
                " 'path'='input/clicks.txt'," +
                " 'format'='csv')");

        Table result = tableEnvironment.sqlQuery("select user_name,url from event_table");

        // 创建输出表
        tableEnvironment.executeSql("CREATE TABLE outTable (" +
                "user_name STRING," +
                "url STRING" +
                ") with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'output'," +
                " 'format' = 'csv'" +
                ")");
        result.executeInsert("outTable");
    }
}

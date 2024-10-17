package com.smile.table;

import org.apache.flink.table.api.*;
import org.apache.flink.table.types.logical.DecimalType;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description
 * @ClassName TableApi
 * @Author smile
 * @date 2023.04.10 21:58
 */
public class TableApi {

    public static void main(String[] args) {
        String salePath = "d:/data/sales.csv";

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        Schema schema = Schema.newBuilder().column("transactionId", DataTypes.INT())
                .column("customerId", DataTypes.INT())
                .column("itemId", DataTypes.INT())
                .column("amountPaid", DataTypes.DECIMAL(2, DecimalType.DEFAULT_SCALE)).build();

        /**
         * 临时表（仅存在flink会话中） 永久表（元数据保存在catalog中）
         */
        tableEnv.createTemporaryTable("sales", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", salePath)
                .format("csv").build());

        Table filterResult = tableEnv.from("sales").where($("customerId").isEqual(1));

        /**
         * table.groupBy(...).select() ，其中 groupBy(...) 指定 table 的分组，而 select(...) 在 table 分组上的投影
         */
        tableEnv.from("sales").groupBy($("customerId"))
                .select($("customerId"), $("amountPaid").sum().as("paid_sum"))
                .execute().print();

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert(TableDescriptor.forConnector("filesystem").schema(schema).option("path", "d:/data/tmp")
                .format("csv").build(), filterResult);

        statementSet.execute();
    }
}

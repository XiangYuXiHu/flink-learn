package com.smile.table;

import org.apache.flink.table.api.*;

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
                .column("amountPaid", DataTypes.STRING()).build();

        tableEnv.createTemporaryTable("sales", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", salePath)
                .format("csv").build());

        Table filterResult = tableEnv.from("sales").where($("customerId").isEqual(1));

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert(TableDescriptor.forConnector("filesystem").schema(schema).option("path", "d:/data/tmp")
                .format("csv").build(), filterResult);

        statementSet.execute();
    }
}

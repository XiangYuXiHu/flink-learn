package com.smile.table;

import com.alibaba.fastjson.JSONObject;
import com.smile.domain.Emp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description
 * @ClassName EmpTable
 * @Author smile
 * @date 2024.10.15 22:09
 */
public class EmpTable {

    public static void main(String[] args) {
        String empPath = "d:/data/emp.txt";

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        DataStream<Emp> source = environment.readTextFile(empPath)
                .map(line -> JSONObject.parseObject(line, Emp.class));

        Table table = tableEnvironment.fromDataStream(source);
        // table.select(Expressions.$("*")).execute().print();

        /**
         * 创建临时视图（临时表），第一个参数是注册的表名（[catalog.db.]tableName）,
         * 第二个参数可以是Tabe对象也可以是DataStream对象，第三个参数是指定的列字段名（可选）。
         */
//        tableEnvironment.createTemporaryView("t_tmp", table);
//        tableEnvironment.sqlQuery("select * from t_tmp").execute().print();

        table.where($("deptno").isEqual(10))
                .select($("ename"),$("job")).execute().print();
        table.groupBy($("deptno")).select($("deptno"),$("sal").avg().as("sal_avg"))
                .execute().print();
    }
}

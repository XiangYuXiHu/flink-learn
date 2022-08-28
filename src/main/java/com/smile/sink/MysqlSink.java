package com.smile.sink;

import com.smile.domain.ProductInfo;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @ClassName MysqlSink
 * @Author smile
 * @date 2022.08.28 08:23
 */
public class MysqlSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<ProductInfo> streamSource = env.fromElements(new ProductInfo("刀具", 8),
                new ProductInfo("化妆品", 9),
                new ProductInfo("枪支", 10));

        String sql = "insert into product_category(category_name,category_type) values(?,?)";
        JdbcConnectionOptions jdbcBuild = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("com.mysql.jdbc.Driver")
                .withUrl("jdbc:mysql://localhost:3306/wechat_order?useUnicode=true&characterEncoding=utf-8&useSSL=false")
                .withUsername("admin").withPassword("admin").build();
        streamSource.addSink(JdbcSink.sink(sql, (ps, productInfo) -> {
            ps.setString(1, productInfo.getCategoryName());
            ps.setInt(2, productInfo.getCategoryType());
        }, jdbcBuild));

        env.execute("jdbc");
    }
}

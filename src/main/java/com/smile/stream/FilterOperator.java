package com.smile.stream;

import com.smile.domain.UserAction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * 过滤操作
 *
 * @Description
 * @ClassName FilterOperator
 * @Author smile
 * @date 2022.08.07 19:53
 */
public class FilterOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        List<UserAction> userActions = Arrays.asList(new UserAction("小明", 10001, "click", "iphone", BigDecimal.valueOf(10)),
                new UserAction("小王", 10002, "browsw", "pc", BigDecimal.valueOf(12)),
                new UserAction("小李", 10003, "click", "mac", BigDecimal.valueOf(10)));
        DataStreamSource<UserAction> source = environment.fromCollection(userActions);
        source.filter((FilterFunction<UserAction>) value -> "小王".equalsIgnoreCase(value.getUserId())).print();

        environment.execute("filter-operator");
    }
}

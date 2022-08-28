package com.smile.sink;

import com.smile.domain.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @ClassName ElasticsearchSink
 * @Author smile
 * @date 2022.08.27 20:24
 */
public class MyElasticsearchSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("192.168.21.128", 9200, "http"));
        ElasticsearchSink.Builder<Event> eventBuilder = new ElasticsearchSink.Builder<>(hosts, new ElasticsearchSinkFunction<Event>() {

            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                Map<String, String> map = new HashMap<>();
                map.put("user", event.getUser());
                map.put("url", event.getUrl());
                IndexRequest indexRequest = Requests.indexRequest();
                indexRequest.index("click-event").source(map);
                requestIndexer.add(indexRequest);
            }
        });

        eventBuilder.setBulkFlushMaxActions(1);
        source.addSink(eventBuilder.build());

        env.execute("click-event");
    }
}

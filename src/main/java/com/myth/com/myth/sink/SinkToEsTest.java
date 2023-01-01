package com.myth.com.myth.sink;

import com.myth.com.myth.customsource.Event;
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

/**
 * 数据下沉：到ES
 */
public class SinkToEsTest {
    public static void main(String[] args) {
        // 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 数据源
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./prod?id=100", 3000L),
                new Event("Mary", "./prod?id=200", 3500L),
                new Event("Mary", "./prod?id=2", 2500L),
                new Event("Mary", "./prod?id=300", 3600L),
                new Event("Mary", "./home", 3000L),
                new Event("Mary", "./prod?id=1", 2300L),
                new Event("Mary", "./prod?id=3", 3300L)
        );

        // ES环境
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost",9200,"http"));

        // 创建一个 ElasticsearchSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<>() {

            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> data = new HashMap<>(16);
                data.put(event.user,event.url);

                IndexRequest request = Requests
                        .indexRequest()
                        .index("clicks")
                        .type("type")
                        .source(data);

                requestIndexer.add(request);
            }
        };

        // 下沉数据
        stream.addSink(new ElasticsearchSink.Builder<Event>(httpHosts,elasticsearchSinkFunction).build());

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


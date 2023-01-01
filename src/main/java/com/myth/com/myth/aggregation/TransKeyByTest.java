package com.myth.com.myth.aggregation;

import com.myth.com.myth.customsource.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransKeyByTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./home", 1000L)
        );

        // 使用lambda表达式
        KeyedStream<Event, String> keyedStream = stream.keyBy(event -> event.user);

        // 使用匿名类实现KeySelector
        KeyedStream<Event, String> keyedStream1 = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        });

        keyedStream.sum("timestamp").print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}

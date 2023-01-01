package com.myth.com.myth.transformation;

import com.myth.com.myth.customsource.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFlatmapTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.flatMap(new MyFlatMap()).print();
        //stream.flatMap(new MyFlatMap2()).print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static class Event2{
        public String user;

        public Boolean flag;

        public Event2() {
        }

        public Event2(String user, Boolean flag) {
            this.user = user;
            this.flag = flag;
        }

        @Override
        public String toString() {
            return "Event2{" +
                    "user='" + user + '\'' +
                    ", flag='" + flag + '\'' +
                    '}';
        }
    }

    private static class MyFlatMap implements FlatMapFunction<Event,String> {
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            if (event.user.equals("Mary")){
                collector.collect(event.user);
            }else if (event.user.equals("Bob")){
                collector.collect(event.user);
                collector.collect(event.url);
            }
        }
    }

    private static class MyFlatMap2 implements FlatMapFunction<Event,Event2> {
        @Override
        public void flatMap(Event event, Collector<Event2> collector) throws Exception {
            if (event.user.equals("Mary")){
                collector.collect(new Event2(event.user,false));
            }else if (event.user.equals("Bob")){
                collector.collect(new Event2(event.user, true));
            }
        }
    }
}

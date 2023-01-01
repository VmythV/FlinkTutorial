package com.myth.com.myth.udf;

import com.myth.com.myth.customsource.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFunctionUDFTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", System.currentTimeMillis()),
                new Event("Mary", "./home", System.currentTimeMillis() + 1000L),
                new Event("Bob", "./home", System.currentTimeMillis() + 3000L),
                new Event("Bob", "./cart", System.currentTimeMillis() + 2000L),
                new Event("Mary", "./cart", System.currentTimeMillis() + 4000L)
        );

        // 自定义 FilterFunction
        SingleOutputStreamOperator<Event> stream = clicks.filter(new FlinkFilter());

        // 匿名内部类
        SingleOutputStreamOperator<Event> stream2 = clicks.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.url.contains("home");
            }
        });

        // 将用于过滤的关键字 "home"抽象出来作为类的属性，调用构造方法时传进去。
        SingleOutputStreamOperator<Event> stream3 = clicks.filter(new KeyWordFilter("home"));
        stream.print();
        stream2.print();
        stream3.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    private static class FlinkFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return event.url.contains("home");
        }
    }

    private static class KeyWordFilter implements FilterFunction<Event> {
        private String keyWord;
        public KeyWordFilter(String keyWord) {
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(Event event) throws Exception {
            return event.url.contains(this.keyWord);
        }
    }
}

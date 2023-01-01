package com.myth.com.myth.udf;

import com.myth.com.myth.customsource.Event;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RichFunctionTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> click = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );

        // 将点击事件转换成长整型的时间戳输出
        click.map(new RichMapFunction<Event, Long>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("parameters:"+parameters);
                System.out.println("索引为 "+ getRuntimeContext().getIndexOfThisSubtask()+" 的任务开始了");
            }

            @Override
            public Long map(Event event) throws Exception {
                return event.timestamp;
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("索引为 "+ getRuntimeContext().getIndexOfThisSubtask()+" 的任务结束了");
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static class MyRichFlatMap extends RichFlatMapFunction<String,String>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 做一些初始化工作
            // 例如：建立一个和MySQL的链接

        }

        @Override
        public void flatMap(String in, Collector<String> collector) throws Exception {

            // 对数据库进行读写

        }

        @Override
        public void close() throws Exception {
            super.close();

            // 清理工作，关闭和MySQL数据库的连接
        }
    }
}

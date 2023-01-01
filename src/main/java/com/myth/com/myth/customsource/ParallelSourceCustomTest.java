package com.myth.com.myth.customsource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ParallelSourceCustomTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //有了自定义的 source function，调用 addSource 方法
        DataStreamSource<Event> stream = env.addSource(new RandomParallelSource()).setParallelism(2);

        stream.print("ParallelSourceCustom");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

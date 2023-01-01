package com.myth.com.myth.physicalpartitioning;

import com.myth.com.myth.customsource.ClickSource;
import com.myth.com.myth.customsource.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 轮询
 */
public class RebalanceTest {
    public static void main(String[] args) {
        // 设置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经轮询重分区打印输出，并行度为4
        stream.rebalance().print("rebalance").setParallelism(4);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

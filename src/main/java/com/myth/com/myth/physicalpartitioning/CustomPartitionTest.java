package com.myth.com.myth.physicalpartitioning;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义分区
 */
public class CustomPartitionTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将自然数按照奇偶分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                /**
                 * 方法需要传入两个参数，
                 * 第一个是自定义分区器（Partitioner）对象，
                 * 第二个 是应用分区器的字段，它的指定方式与 keyBy 指定 key 基本一样：可以通过字段名称指定， 也可以通过字段位置索引来指定，还可以实现一个 KeySelector。
                 */
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int i) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print()
                .setParallelism(2);

        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new MyCustomPartitioner(), new MyCustomKeySelector())
                .print()
                .setParallelism(2);


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class MyCustomPartitioner implements Partitioner<Integer> {

        @Override
        public int partition(Integer key, int i) {
            return key % 2;
        }
    }

    public static class MyCustomKeySelector implements KeySelector<Integer,Integer> {

        @Override
        public Integer getKey(Integer value) throws Exception {
            return value;
        }
    }
}

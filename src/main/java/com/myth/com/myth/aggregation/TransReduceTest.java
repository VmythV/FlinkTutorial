package com.myth.com.myth.aggregation;

import com.myth.com.myth.customsource.ClickSource;
import com.myth.com.myth.customsource.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransReduceTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用自定义数据源
        env.addSource(new ClickSource())
                // 将Event数据类型转化成元组类型
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                })
                // 使用 用户名来进行分流
                .keyBy(r -> r.f0)
                // reduce 每得到一条数据，用户pv的统计值累加
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        /**
                         * v1 累计值
                         * v2 当前值
                         */
                        return Tuple2.of(v1.f0, v1.f1 + v2.f1);
                    }
                })
                // 为每一条数据分类同一个key，将聚合结果发送到一条流中去
                .keyBy(r->true)
                // 将累加器更新为当前最大的pv统计值，然后向下游发送累加器的值（将每个人聚合后的结果再聚合出所有人中最大的值）
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        return v1.f1 > v2.f1 ? v1:v2;
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

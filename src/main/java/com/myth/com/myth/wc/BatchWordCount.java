package com.myth.com.myth.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) {

        // 创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 获取数据源
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        // 将每一行数据进行分词，转化成二元组
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将每一行数据进行分词
            String[] words = line.split(" ");
            for (String word : words) {
                // 将每一个单词转换成二元组输出
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

        // 分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        try {
            // 打印结果
            sum.print();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

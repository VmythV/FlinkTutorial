package com.myth.com.myth.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.stream.Collectors;

public class StreamWordCount {
    public static void main(String[] args) {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取服务器实时发送的文本流
        // 如果没有服务器可以使用命令行工具netcat（需要安装），也可以docker安装ubuntu，开启端口映射，然后ubuntu执行命令
        // apt update -y    -> apt install netcat -y     -> nc -lk 7777
        DataStreamSource<String> lineDSS = env.socketTextStream("localhost", 7777);

        // 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS.flatMap((String line, Collector<String> words) -> {
            Arrays.stream(line.split(" h")).forEach(words::collect);
        }).returns(Types.STRING).map(word -> Tuple2.of(word, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOne.keyBy(data -> data.f0);

        // 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        // 打印
        sum.print();

        // 执行
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}

package com.myth.com.myth.sink;

import com.myth.com.myth.customsource.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据下沉：到MySQL
 */
public class SinkToMySQLTest {
    public static void main(String[] args) {
        // 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 数据源
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./prod?id=100", 3000L),
                new Event("Mary", "./prod?id=200", 3500L),
                new Event("Mary", "./prod?id=2", 2500L),
                new Event("Mary", "./prod?id=300", 3600L),
                new Event("Mary", "./home", 3000L),
                new Event("Mary", "./prod?id=1", 2300L),
                new Event("Mary", "./prod?id=3", 3300L)
        );

        /**
         * 数据表
         * create table clicks(
         *  user varchar(20) not null,
         *  url varchar(100) not null
         * )
         */
        stream.addSink(
                JdbcSink.sink(
                "INSERT INTO clicks (user, url) VALUES (?, ?)",
                (statement, event) -> {
                    statement.setString(1, event.user);
                    statement.setString(2, event.url);
                },
                JdbcExecutionOptions
                        .builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/userbehavior")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("username")
                        .withPassword("password")
                        .build()
                )
        );

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

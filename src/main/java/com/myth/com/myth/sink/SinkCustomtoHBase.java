package com.myth.com.myth.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

/**
 * 自定义下沉数据：hbase
 */
public class SinkCustomtoHBase {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("hello", "world")
                .addSink(
                        new RichSinkFunction<String>() {

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                configuration = HBaseConfiguration.create();
                                configuration.set("hbase.zookeeper.quorum","localhost:2181");

                                connection = ConnectionFactory.createConnection(configuration);
                            }

                            @Override
                            public void invoke(String value, Context context) throws Exception {
                                // 表名为test
                                Table table = connection.getTable(TableName.valueOf("test"));
                                // 指定rowkey
                                Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8));

                                put.addColumn(
                                        // 指定列名
                                        "info".getBytes(StandardCharsets.UTF_8),
                                        // 写入的数据
                                        value.getBytes(StandardCharsets.UTF_8),
                                        // 写入的数据
                                        "1".getBytes(StandardCharsets.UTF_8)
                                );
                                // 执行put操作
                                table.put(put);
                                // 将表关闭
                                table.close();
                            }

                            @Override
                            public void close() throws Exception {
                                super.close();
                                // 关闭连接
                                connection.close();
                            }

                            /**
                             * 管理 Hbase 的配置信息,这里因为 Configuration 的重名问题，将类以完整路径导入
                             */
                            public org.apache.hadoop.conf.Configuration configuration;

                            /**
                             * 管理 Hbase 连接
                             */
                            public Connection connection;



                        }
                );

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

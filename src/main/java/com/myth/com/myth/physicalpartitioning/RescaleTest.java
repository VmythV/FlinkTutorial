package com.myth.com.myth.physicalpartitioning;

import com.myth.com.myth.customsource.ClickSource;
import com.myth.com.myth.customsource.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 重缩放
 */
public class RescaleTest {
    public static void main(String[] args) {
        // 设置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 增加数据源
        // 这里使用了并行数据源的富函数版本
        // 这样可以调用getRuntimeContext方法来获取运行时上下文的一些信息
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 0; i < 8; i++) {
                    // 将奇数发送到索引为1的并行子任务
                    // 将偶数发送到索引为0的并行子任务
                    if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        sourceContext.collect(i + 1);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).rescale().print().setParallelism(4);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

package com.myth.com.myth.customsource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class RandomParallelSource implements ParallelSourceFunction<Event> {

    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        // 在指定的数据集中随机选取数据
        Random random = new Random();

        String[] users = {"Mary", "Alice", "Bob", "Cary"};

        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (Boolean.TRUE.equals(running)) {
            sourceContext.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));

            // 隔 1 秒生成一个点击事件，方便观测
            Thread.sleep(1000);

        }
    }

    @Override
    public void cancel() {

    }
}

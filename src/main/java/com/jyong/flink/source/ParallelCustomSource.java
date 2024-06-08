package com.jyong.flink.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @author: jyong
 * @description 自定义并行数据源
 * @date: 2023/3/20 21:11
 */
public class ParallelCustomSource implements ParallelSourceFunction<Integer> {

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        Random random = new Random();
        while (true){
            sourceContext.collect(random.nextInt());
        }
    }

    @Override
    public void cancel() {

    }
}

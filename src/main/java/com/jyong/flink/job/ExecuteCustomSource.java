package com.jyong.flink.job;

import com.jyong.flink.source.ClickSource;
import com.jyong.flink.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: jyong
 * @description 调用自定义source
 * @date: 2023/3/20 20:53
 */
public class ExecuteCustomSource {
    public static void main(String[] args) throws Exception {

        //1.获取执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.调用source
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
//        DataStreamSource<Integer> eventDataStreamSource = env.addSource(new ParallelCustomSource()).setParallelism(4);

        //3.打印
        eventDataStreamSource.print();

        env.execute();
    }
}

package com.jyong.flink.job.window;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author: jyong
 * @description watermark案例
 * @date: 2023/3/30 20:34
 */
public class WaterMarkDemo {


    public static void main(String[] args) throws Exception {

        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(100);
        //2.从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("zhangsan", "/index", 1000L),
                new Event("lisi", "/cat", 1000L),
                new Event("zhangsan", "/cat", 1000L),
                new Event("zhangsan", "/cat", 2000L),
                new Event("lisi", "/cat", 3000L),
                new Event("tianliu", "/cat", 4000L),
                new Event("wangwu", "/index", 1000L)
        );

        //3.在靠近DataStreamSource数据源的地方设置watermark

        //3.1有序流的watermark生成 : forMonotonousTimestamps
        eventDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(
                new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }
        ));

        //3.2乱序流的watermark的生成:forBoundedOutOfOrderness
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = eventDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                //设置最大的延迟时间
                Duration.ofMillis(100)
        ).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }
        }));

        eventSingleOutputStreamOperator.print();

        env.execute();


    }

}

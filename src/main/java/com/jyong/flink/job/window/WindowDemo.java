package com.jyong.flink.job.window;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * @author: jyong
 * @description 窗口
 * @date: 2023/3/30 21:33
 */
public class WindowDemo {


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

        //乱序流的watermark的生成:forBoundedOutOfOrderness
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = eventDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                //设置最大的延迟时间
                Duration.ofMillis(100)
        ).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }
        }));

        //4.窗口操作

        //4.1滚动窗口：TumblingWindowTimeWindow
        SingleOutputStreamOperator<Event> result = eventSingleOutputStreamOperator.keyBy(Event::getUser)
                .window(EventTimeSessionWindows.withGap(Time.hours(1)))
//                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.milliseconds(5)))//滑动事件时间窗口
//                .window(  .of(Time.milliseconds(5)))//滚动事件时间窗口
                .sum("timestamp");

        result.print();

        env.execute();


    }
}

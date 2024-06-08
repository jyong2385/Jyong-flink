package com.jyong.flink.job.window;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.time.Duration;

/**
 * @author: jyong
 * @description 移除器
 * @date: 2023/4/5 15:57
 */
public class EvictorExample {

    public static void main(String[] args) throws Exception {



        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(100);

        //2.从元素中读取数据
        DataStream<Event> eventDataStreamSource = env.fromElements(new Event("zhangsan", "/index", 1000L),
                        new Event("lisi", "/cat", 1000L),
                        new Event("zhangsan", "/cat", 1000L),
                        new Event("zhangsan", "/cat", 2000L),
                        new Event("lisi", "/cat", 3000L),
                        new Event("tianliu", "/cat", 4000L),
                        new Event("wangwu", "/index", 1000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimestamp();
                            }
                        })
                );

        eventDataStreamSource.print("data->");
        //3.使用AggregateFunction 和 ProcessWindowFunction
        eventDataStreamSource.keyBy(data->true)
                //移除器：evic
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .evictor(new CustomEvitorBefore())
                //允许延迟的时间
                .allowedLateness(Time.milliseconds(2))
                .sum("timestamp")
                .print();

        env.execute();
    }

    private static class CustomEvitorBefore implements Evictor<Event, TimeWindow> {
        @Override
        public void evictBefore(Iterable<TimestampedValue<Event>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Event>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

        }
    }
}

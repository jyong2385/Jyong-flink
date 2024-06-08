package com.jyong.flink.job.window;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @author: jyong
 * @description
 * @date: 2023/4/3 20:30
 * WindowProcessFunction:
 * 1.携带窗口信息
 * 2.全窗口数据聚合
 * 3.无法增量聚合
 */
public class WindowProcessExample {

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

        //3.使用ProcessWindowFunction
        eventDataStreamSource.keyBy(ele -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new CustomProcessWindowFunction())
                .print();


        //4.触发执行
        env.execute();


    }


    //实现自定义的ProcessWindowFunction，输出一条统计信息
    private static class CustomProcessWindowFunction extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
            HashSet<String> set = new HashSet<>();
            for (Event event : iterable) {
                set.add(event.getUser());
            }

            int uv = set.size();
            //结合窗口信息输出
            long start = context.window().getStart();
            long end = context.window().getEnd();


            //输出
            collector.collect("窗口：" + new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " uv值为：" + uv
            );
        }
    }
}

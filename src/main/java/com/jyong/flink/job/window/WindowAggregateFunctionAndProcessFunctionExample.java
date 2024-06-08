package com.jyong.flink.job.window;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
 * @date: 2023/4/5 09:04
 *
 * AggregateFunction 和 ProcessWindowFunction 一起使用，既能获取到窗口信息，又能增量聚合
 *
 */
public class WindowAggregateFunctionAndProcessFunctionExample {


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
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .aggregate(new CustomAggregateFunction(),new CustomProcessWindowFunction())
                .print();

        env.execute();


    }

    /**
     *自定义实现AggregateFunction,增量聚合计算UV值
     */

    public static class CustomAggregateFunction implements AggregateFunction<Event, HashSet<String>,Integer>{

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<String>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> accumulator) {
            accumulator.add(event.getUser());
            return accumulator;
        }

        @Override
        public Integer getResult(HashSet<String> accumulator) {
            return accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }



    /**
     *自定义实现ProcessWindowFunction,结合输出的结果包装窗口信息
     */

    public static class CustomProcessWindowFunction extends ProcessWindowFunction<Integer,String,Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Integer, String, Boolean, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<String> collector) throws Exception {

            long start = context.window().getStart();
            long end = context.window().getEnd();


            Integer uv = iterable.iterator().next();

            //输出
            collector.collect("窗口：" + new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " uv值为：" + uv
            );

        }
    }


}

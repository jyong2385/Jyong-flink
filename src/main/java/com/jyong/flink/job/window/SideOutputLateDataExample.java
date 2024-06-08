package com.jyong.flink.job.window;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author: jyong
 * @description 侧输出流
 * @date: 2023/4/5 16:13
 */
public class SideOutputLateDataExample {

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

        //定义一个数据标签
        OutputTag<Event> late = new OutputTag<>("late");

        eventDataStreamSource.print("data->");
        //3.使用AggregateFunction 和 ProcessWindowFunction
        SingleOutputStreamOperator<String> outputStreamOperator = eventDataStreamSource.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                //侧输出流
                .sideOutputLateData(late)
                .aggregate(new WindowAggregateFunctionAndProcessFunctionExample.CustomAggregateFunction(), new WindowAggregateFunctionAndProcessFunctionExample.CustomProcessWindowFunction());

        //侧输出流的数据处理
        outputStreamOperator.getSideOutput(late).print("late data process");


        env.execute();
    }


}

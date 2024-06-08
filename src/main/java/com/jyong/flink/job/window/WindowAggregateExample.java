package com.jyong.flink.job.window;

import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author: jyong
 * @description Aggregate聚合函数
 * @date: 2023/4/2 15:24
 */
public class WindowAggregateExample {

    public static void main(String[] args) throws Exception {

        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(100);
        //2.从自定义数据源中读取数据
        SingleOutputStreamOperator<Event> eventDataStreamSource = env.addSource(new ClickSource())
                //3.设置乱序流的watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimestamp();
                            }
                        })
                );


        //3.agg函数逻辑
        /**
         * 输出每个用户的平均访问时间
         */
        SingleOutputStreamOperator<String> streamOperator = eventDataStreamSource.keyBy(e -> e.getUser())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(20)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() {
                    @Override
                    public Tuple2<Long, Integer> createAccumulator() {
                        return Tuple2.of(0L, 0);
                    }

                    @Override
                    public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> accumulator) {
                        return Tuple2.of(event.getTimestamp() + accumulator.f0, accumulator.f1 + 2);
                    }

                    @Override
                    public String getResult(Tuple2<Long, Integer> result) {
                        //求平均数
                        Timestamp timestamp = new Timestamp(result.f0 / result.f1);
                        return timestamp.toString();
                    }

                    @Override
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> acc1, Tuple2<Long, Integer> acc2) {
                        return Tuple2.of(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
                    }
                });

        //4.触发执行
        env.execute();


    }


}

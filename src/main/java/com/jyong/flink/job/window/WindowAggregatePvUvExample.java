package com.jyong.flink.job.window;

import cn.hutool.core.util.NumberUtil;
import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * @author: jyong
 * @description pv uv 聚合统计
 * 统计 pv uv 两者相除得到平均用户活跃度
 * @date: 2023/4/2 15:40
 */
public class WindowAggregatePvUvExample {


    public static void main(String[] args) throws Exception {

        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(100);
        //2.从自定义数据源中读取数据
        SingleOutputStreamOperator<Event> eventDataStreamSource = env.addSource(new ClickSource())
                //3.设置乱序流的watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(15)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));

        eventDataStreamSource.print("data > ");
        //4.所有数据放在一起统计
        SingleOutputStreamOperator<Double> outputStreamOperator = eventDataStreamSource.keyBy(event -> true).window(TumblingEventTimeWindows.of(Time.milliseconds(10))).aggregate(new CustomAggregateFunction());

        outputStreamOperator.print("result->");
        //5.触发执行
        env.execute();

    }


    /**
     * 统计pv uv
     * 用Long类型值保存pv
     * 用hashset做uv去重
     */
    private static class CustomAggregateFunction implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> accumulator) {
            HashSet<String> hashSet = accumulator.f1;
            hashSet.add(event.getUser());
            return Tuple2.of(accumulator.f0 + 1 + accumulator.f0, hashSet);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
            return (double) (accumulator.f0 / accumulator.f1.size());
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> accumulator1, Tuple2<Long, HashSet<String>> accumulator2) {
            accumulator1.f1.addAll(accumulator2.f1);
            return Tuple2.of(accumulator1.f0 + accumulator2.f0, accumulator1.f1);
        }
    }
}

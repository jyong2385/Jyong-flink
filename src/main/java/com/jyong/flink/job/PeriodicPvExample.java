package com.jyong.flink.job;

import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/5/14 14:10
 * @desc 需求：统计每个用户的pv值，并且每隔10秒输出一次
 */

public class PeriodicPvExample {

    public static void main(String[] args) throws Exception {

        //1. 创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.数据源
        SingleOutputStreamOperator<Event> dataSource = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                        Duration.ZERO
                ).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));

        //4.统计每个用户点击量-pv
        dataSource.keyBy(data -> data.getUser())
                .process(new PeriodicPvProcess())
                .print();


        //触发执行
        env.execute();


    }

    ///实现自定义的KeyedProcessFuntion
    public static class PeriodicPvProcess extends KeyedProcessFunction<String, Event, String> {

        //定义状态，保存当前的pv统计值,以及有没有定时器
        ValueState<Long> countState;

        ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));

        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

            /**
             * 逻辑处理：每来一条数据，就更新对应的count的值
             *
             */
            Long count = countState.value();
            countState.update(count == null ? 1 : count + 1);

            //如果没有注册过定时器的话，就进行注册定时器
            if (timerState.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getTimestamp() + 10 * 1000L); //当前时间为基础，注册一个10秒的定时器
                timerState.update(value.getTimestamp() + 10 * 1000L);
            }

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发的逻辑
            out.collect("timestamp: " +new Timestamp(timestamp) +" "+ ctx.getCurrentKey()+" pv: "+countState.value());
            //清空状态
//            countState.clear();
            timerState.clear();
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timerState.update(timestamp + 10 * 1000L);
        }

    }


}

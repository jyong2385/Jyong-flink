package com.jyong.flink.job;

import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/5/15 20:46
 * @desc 利用MapState模拟window操作
 */

public class FakeWindowExample {

    public static void main(String[] args) throws Exception {

        //1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);


        //3.引入数据源
        DataStream<Event> eventDataStreamSource = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                }));


        //4.分组处理
        eventDataStreamSource.keyBy(data -> data.getUser())
                .process(new FakeWindowResult(10000L))
                .print();


        //触发执行
        env.execute();

    }

    //实现自定义的KeyedProcessFunction
    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {

        //窗口大小
        private Long windowSize;

        //定义一个MapState，用来保存每个窗口中统计的count值
        MapState<Long, Long> windowUrlCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-state", Long.class, Long.class));
        }

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

            //每来一条数据，根据时间戳判断属于哪个窗口（窗口分配器）
            Long windowStart = value.getTimestamp() / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            //注册end-1定时器
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            //更新状态，进行增量聚合
            if (windowUrlCountMapState.contains(windowStart)) {
                Long count = windowUrlCountMapState.get(windowStart);
                windowUrlCountMapState.put(windowStart, count + 1);
            } else {
                windowUrlCountMapState.put(windowStart, 1L);
            }

        }
        //定时器触发时输出计算结果


        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long count = windowUrlCountMapState.get(windowStart);
            //打印
            out.collect("窗口：" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd)
                    + " url: " + ctx.getCurrentKey()
                    + " count: " + count
            );

            //模拟窗口的关闭，移除mapstate中的对应的key-value
            windowUrlCountMapState.remove(windowStart);

        }
    }


}

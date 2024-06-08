package com.jyong.flink.job;

import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/5/17 20:39
 * @desc 状态的ttl配置
 */

public class StateTtlExample {


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

        //4.处理
        eventDataStreamSource.keyBy(data -> data.getUser()).flatMap(new MyFlatMap()).print();

        //4. 触发
        env.execute();


    }

    /**
     * 自定义MyFlatMap，测试Keyed State
     */
    private static class MyFlatMap extends RichFlatMapFunction<Event, String> {

        //定义状态
        ValueState<Event> valueState;
        ListState<Event> listState;

        MapState<String, Long> mapState;

        ReducingState<Event> reducingState;

        AggregatingState<Event, String> aggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("value-state", Event.class));

            listState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("list-state", Event.class));

            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state", String.class, Long.class));

            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("reducing-state", new ReduceFunction<Event>() {
                @Override
                public Event reduce(Event value1, Event value2) throws Exception {
                    value1.setTimestamp(value2.getTimestamp());
                    return value1;
                }
            }, Event.class));


            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("agg-state", new AggregateFunction<Event, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(Event value, Long accumulator) {
                    return accumulator + 1;
                }

                @Override
                public String getResult(Long accumulator) {
                    return "result " + accumulator;
                }

                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            }, Long.class));

            //todo 配置状态的ttl

            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("value-state-test", Event.class);
            //初始化状态

            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.hours(1L))
                    /**
                     * 更新状态生效时间的操作
                     *  OnCreateAndWrite ： 创建状态和写状态
                     *  OnReadAndWrite ： 读取状态和写状态
                     */
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    /**
                     * 状态的可见性
                     * NeverReturnExpired  状态失效后，读取状态的时候，不返回失效的数据
                     * ReturnExpiredIfNotCleanedUp  状态失效后，读取状态的时候，如果状态没有清除，则返回失效的状态
                     */
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)

                    .build();
            valueStateDescriptor.enableTimeToLive(stateTtlConfig);





        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {

            //访问state
//            System.out.println(valueState.value());
            valueState.update(value);
            System.out.println("valueState " + valueState.value());

            listState.add(value);
            System.out.println("listState " + listState);

            mapState.put(value.getUser(), mapState.get(value.getUser()) == null ? 1 : mapState.get(value.getUser()) + 1);
            System.out.println("mapState " + value.getUser() + "  " + mapState.get(value.getUser()));

            reducingState.add(value);
            System.out.println("reducingState " + reducingState.get());


            aggregatingState.add(value);
            System.out.println("aggState " + aggregatingState.get());


            //状态清除，在一定条件下，将状态进行初始化到最初状态
            if (value.getUser() == null) {
                mapState.clear();
            }

        }
    }





}

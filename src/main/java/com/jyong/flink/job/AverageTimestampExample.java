package com.jyong.flink.job;

import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/5/15 21:06
 * @desc 利用aggstate 实现平均时间戳的统计
 */

public class AverageTimestampExample {

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

        eventDataStreamSource.print("input> ");

        //4.分组处理
        eventDataStreamSource.keyBy(data -> data.getUser())
                .flatMap(new AvgTsResult(5L))
                .print();


        //触发执行
        env.execute();

    }

    //实现自定义的RichFlatmapFunction
    public static class AvgTsResult extends RichFlatMapFunction<Event, String> {

        //设置一个阈值，当达到一定数量的时候进行输出
        private Long count;

        public AvgTsResult(Long count) {
            this.count = count;
        }

        //定义一个聚合的状态，用来保存平均时间戳
        AggregatingState<Event, Long> avgTsAggState;
        //定义一个valueState保存用户访问的次数
        ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {

            avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "avgts-state",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(value.getTimestamp() + accumulator.f0, accumulator.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)));

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //每来一条数据，curr count +1
            Long currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 1L;
            } else {
                currentCount++;
            }
            //更新状态
            countState.update(currentCount);
            avgTsAggState.add(value);

            //如果达到count次数就输出结果
            if (currentCount.equals(count)) {
                out.collect("result:"+value.getUser() + "过去" + count + "次访问平均时间戳为：" + avgTsAggState.get());
                //清空状态
                countState.clear();
                avgTsAggState.clear();
            }

        }
    }


}

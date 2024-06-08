package com.jyong.flink.job;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author jyong
 * @Date 2023/5/14 21:37
 * @desc 两条流实现全外联结
 */

public class TwoStreamJoinExample {

    public static void main(String[] args) throws Exception {

        //1. 创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.定义流
        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamOperator1 = env.fromElements(
                        Tuple3.of("a", "stream-1", 1000L),
                        Tuple3.of("b", "stream-1", 2000L),
                        Tuple3.of("a", "stream-1", 3000L),
                        Tuple3.of("c", "stream-1", 3000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                }));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamOperator2 = env.fromElements(
                        Tuple3.of("a", "stream-2", 3000L),
                        Tuple3.of("b", "stream-2", 4000L),
                        Tuple3.of("a", "stream-2", 6000L),
                        Tuple3.of("d", "stream-2", 6000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                }));


        //4.自定义列表状态进行全外联结
        streamOperator1.keyBy(data -> data.f0)
                .connect(streamOperator2.keyBy(data -> data.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {


                    //定义列表状态，用于保存两条流中已经到达的所有数据
                    ListState<Tuple2<String, Long>> stream1ListState;
                    ListState<Tuple2<String, Long>> stream2ListState;


                    @Override
                    public void open(Configuration parameters) throws Exception {

                        //初始化状态
                        stream1ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("stream1-state", Types.TUPLE(Types.STRING, Types.LONG)));
                        stream2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("stream2-state", Types.TUPLE(Types.STRING, Types.LONG)));

                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {

                        //获取另外一条流中所有数据，配对输出
                        for (Tuple2<String, Long> right : stream2ListState.get()) {

                            out.collect(left.f0 + " " + left.f2 + " =>" + right);

                        }

                        //将本条数据添加到本地状态中
                        stream1ListState.add(Tuple2.of(left.f0, left.f2));

                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {

                        //获取另外一条流中的所有数据，配对输出
                        for (Tuple2<String, Long> left : stream1ListState.get()) {

                            out.collect(left + " =>" + right.f0 + " " + right.f2);

                        }

                        //将本条数据添加到本地状态中
                        stream2ListState.add(Tuple2.of(right.f0, right.f2));

                    }
                }).print();


        //5.触发执行
        env.execute();

    }
}

/**
 * 输出结果：
 * (a,1000) =>a 3000
 * (b,2000) =>b 4000
 * a 3000 =>(a,3000)
 * (a,1000) =>a 6000
 * (a,3000) =>a 6000
 */
package com.jyong.flink.job;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/5/5 21:46
 * @desc 合流对账示例
 */

public class BillCheckExample {

    public static void main(String[] args) throws Exception {
        //1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.获取流：来自app的支付日志
        DataStream<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> tuple3, long l) {
                        return tuple3.f2;
                    }
                }));

        //3.1 获取流：来自第三方平台的支付日志
        DataStream<Tuple4<String, String, String, Long>> thirdPartyStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 1000L),
                Tuple4.of("order-3", "third-party", "success", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> tuple4, long l) {
                        return tuple4.f3;
                    }
                }));

        //4。检测同一支付单在两条流中是否匹配，不匹配就报警,超时时间为5秒

        //方法1
//        appStream.keyBy(data -> data.f0)
//                .connect(thirdPartyStream.keyBy(data -> data.f0));

        //方法2
        appStream.connect(thirdPartyStream)
                .keyBy(data -> data.f0, data -> data.f0)
                //定时器
                .process(new OrderMatchResult())
                .print();



        env.execute();


    }

    //自定义实现CoProcessFunction
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

        //定义状态变量用来保持已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //存储状态
            appEventState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thirdPartEventState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String, String, String, Long>>("third-part-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> appData, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {

            //处理app数据，看另一条流中是否来过
            if (thirdPartEventState.value() != null) {
                collector.collect("对账成功： " + appData + "  " + thirdPartEventState.value());
                //清空状态
                thirdPartEventState.clear();
            } else {
                //更新状态
                appEventState.update(appData);

                //注册一个5秒定时器，开始等待另外一条流的事件
                context.timerService().registerEventTimeTimer(appData.f2 + 5000L);
            }

        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> thirdPartData, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {

            //处理支付平台数据，看另一条流中是否来过
            if (appEventState.value() != null) {
                collector.collect("对账成功： " + appEventState.value() + " " + thirdPartData);
                //清空状态
                appEventState.clear();
            } else {
                //更新状态
                thirdPartEventState.update(thirdPartData);

                //注册一个3秒定时器，开始等待另外一条流的事件
                context.timerService().registerEventTimeTimer(thirdPartData.f3);
            }

        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，判断状态，如果某个状态不为空，说明另外一个流中的事件没有来,并且超时了
            if(appEventState != null ){
                out.collect("对账失败："+appEventState.value()+"  第三方支付平台信息未到");
            }

            if(thirdPartEventState != null){
                out.collect("对账失败："+thirdPartEventState.value()+" app支付信息未到");
            }

            appEventState.clear();
            thirdPartEventState.clear();

        }
    }


}

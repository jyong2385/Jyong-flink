package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.OrderLogModel;
import com.jyong.flink.op_case.beans.OrderResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/6/24 17:31
 * @desc 检测订单超时-利用普通process处理方式
 */

public class CheckOrderPayTimeoutProccess {

    private final static OutputTag<OrderResult> OUTPUT_TAG = new OutputTag<OrderResult>("timeout") {
    };

    public static void main(String[] args) throws Exception {
        //1.坏境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从自定义数据源中读取数据
        SingleOutputStreamOperator<OrderLogModel> dataSource = env.readTextFile("/Users/jyong/Desktop/jyong/workplace/coding/Jyong/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/data/OrderLog.csv")
                .map(new MapFunction<String, OrderLogModel>() {
                    @Override
                    public OrderLogModel map(String s) throws Exception {
                        String[] splits = s.split(",");

                        //34747,pay,329d09f9f,1558430893

                        String userId = splits[0];
                        String action = splits[1];
                        String tx = splits[2];
                        Long timestamp = Long.valueOf(splits[3]);
                        return new OrderLogModel(userId, action, tx, timestamp);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderLogModel>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderLogModel>() {
                            @Override
                            public long extractTimestamp(OrderLogModel orderLogModel, long l) {
                                return orderLogModel.getTimestamp() * 1000;
                            }
                        }));

        //自定义处理函数，主流输出正常匹配事件，侧输出流输出超时报警事件
        SingleOutputStreamOperator<OrderResult> process = dataSource.keyBy(OrderLogModel::getUserId)
                .process(new MyProcessFunction());

        DataStream<OrderResult> orderResultDataStream = process.getSideOutput(OUTPUT_TAG);


        //7.打印输出
        process.print("payed normally");
        orderResultDataStream.print("timeout");

        //8.触发执行
        env.execute();

    }


    //自定义实现KeyedProcessFunction
    private static class MyProcessFunction extends KeyedProcessFunction<String, OrderLogModel, OrderResult> {

        //定义状态保存订单是否创建或支付
        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreateState;

        //保存定时器时间戳的状态
        ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {

            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isPayedState", Boolean.class));
            isCreateState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isCreateState", Boolean.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));

        }

        @Override
        public void processElement(OrderLogModel value, KeyedProcessFunction<String, OrderLogModel, OrderResult>.Context ctx, Collector<OrderResult> out) throws Exception {

            //获取当前的状态
            boolean isPayed = isPayedState.value() != null && isPayedState.value();
            boolean isCreate = isCreateState.value() != null && isCreateState.value();
            long timestamp = timerState.value() == null ? 0L : timerState.value();


            //判断当前事件类型
            if ("create".equalsIgnoreCase(value.getAction())) {

                //如果是create，需要判断是否是已经支付过的订单，乱序数据情况下
                if (isPayed) {
                    //如果已经支付过了，则正常输出结果
                    out.collect(new OrderResult(value.getUserId(), "payed successfully"));
                    //清空状态
                    isPayedState.clear();
                    isCreateState.clear();
                    timerState.clear();

                    //删除定时器
                    ctx.timerService().deleteEventTimeTimer(timestamp);
                } else {
                    //没有支付过，则注册15分钟后的定时器，开始等待支付事件
                    Long ts = (value.getTimestamp() + 15 * 60) * 1000;
                    ctx.timerService().registerEventTimeTimer(ts);
                    //更新状态
                    timerState.update(ts);
                    isCreateState.update(true);
                }
            } else if ("pay".equalsIgnoreCase(value.getAction())) {

                //如果是pay，需要判断是否是已经订单过的订单，乱序数据情况下
                if (isCreate) {

                    //需要判断是否是超过15分钟后的订单数据
                    if (value.getTimestamp() * 1000 < timestamp) {
                        //正常数据，则进行正常输出
                        out.collect(new OrderResult(value.getUserId(), "payed successfully"));
                    } else {

                        //已经超时的数据，输出到侧输出流中
                        ctx.output(OUTPUT_TAG, new OrderResult(value.getUserId(), "timeout"));
                    }

                    //统一处理
                    //删除定时器
                    ctx.timerService().deleteEventTimeTimer(timestamp);
                    //清空状态
                    isPayedState.clear();
                    isCreateState.clear();
                    timerState.clear();


                } else {
                    //没有下单事件，需要注册一个定时器等待
                    ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000L);

                    //更新状态
                    timerState.update(value.getTimestamp() * 1000L);
                    isPayedState.update(true);

                }
            }

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, OrderLogModel, OrderResult>.OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            //定时器触发，说明有一个事件没来

            if (isPayedState.value() != null && isPayedState.value()) {
                //支付，但未下单
                out.collect(new OrderResult(ctx.getCurrentKey(), "payed but not create"));
            } else if (isCreateState.value() != null && isCreateState.value()) {
                //支付超时
                out.collect(new OrderResult(ctx.getCurrentKey(), "created but not payed"));
            } else {
                //支付超时
                out.collect(new OrderResult(ctx.getCurrentKey(), "error data"));
            }

            //清空状态
            isPayedState.clear();
            isCreateState.clear();
            timerState.clear();
        }
    }


}

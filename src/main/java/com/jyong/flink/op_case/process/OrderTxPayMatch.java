package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.OrderLogModel;
import com.jyong.flink.op_case.beans.OrderTxResult;
import com.jyong.flink.op_case.beans.ReceiptLogModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/6/25 21:32
 * @desc 平台对账：来自两条流的订单交易数据
 */

public class OrderTxPayMatch {

    /**
     * 订单事件中未匹配到的数据
     */
    private static final OutputTag<OrderLogModel> OUTPUT_TAG_ORDER = new OutputTag<OrderLogModel>("OUTPUT_TAG_ORDER") {
    };

    /**
     * 到账数据中未匹配到的数据
     */
    private static final OutputTag<ReceiptLogModel> OUTPUT_TAG_RECEIPT = new OutputTag<ReceiptLogModel>("OUTPUT_TAG_RECEIPT") {
    };

    public static void main(String[] args) throws Exception {
        //1.坏境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取订单数据
        SingleOutputStreamOperator<OrderLogModel> orderLogDataSource = env.readTextFile("/Users/jyong/Desktop/jyong/workplace/coding/Jyong/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/data/OrderLog.csv")
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
                .filter(s->StringUtils.isNotBlank(s.getTxId()))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderLogModel>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderLogModel>() {
                            @Override
                            public long extractTimestamp(OrderLogModel orderLogModel, long l) {
                                return orderLogModel.getTimestamp() * 1000;
                            }
                        }));


        //2.1读取到账数据
        SingleOutputStreamOperator<ReceiptLogModel> receiptLogDataSource = env.readTextFile("/Users/jyong/Desktop/jyong/workplace/coding/Jyong/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/data/ReceiptLog.csv")
                .map(new MapFunction<String, ReceiptLogModel>() {
                    @Override
                    public ReceiptLogModel map(String s) throws Exception {
                        String[] splits = s.split(",");

                        //34747,pay,329d09f9f,1558430893
                        String txId = splits[0];
                        String channel = splits[1];
                        Long timestamp = Long.valueOf(splits[2]);
                        return new ReceiptLogModel(txId, channel, timestamp);
                    }
                })
                .filter(s->StringUtils.isNotBlank(s.getTxId()))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ReceiptLogModel>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ReceiptLogModel>() {
                            @Override
                            public long extractTimestamp(ReceiptLogModel orderLogModel, long l) {
                                return orderLogModel.getTimestamp() * 1000;
                            }
                        }));


        //3.双流连接join,进行匹配处理，不匹配的事件输出到侧输出流
        SingleOutputStreamOperator<OrderTxResult> result = orderLogDataSource.keyBy(OrderLogModel::getTxId)
                .connect(receiptLogDataSource.keyBy(ReceiptLogModel::getTxId))
                .process(new TxPayMatchDetect());


        //4.打印输出
        result.print("matched-pays");
        result.getSideOutput(OUTPUT_TAG_ORDER).print("un-match-order");
        result.getSideOutput(OUTPUT_TAG_RECEIPT).print("un-match-receipt");

        //5.触发
        env.execute();


    }


    //自定义合流处理操作 实现CoProcessFunction
    private static class TxPayMatchDetect extends CoProcessFunction<OrderLogModel, ReceiptLogModel, OrderTxResult> {

        //定义状态，保存支付事件和到账事件
        ValueState<OrderLogModel> orderLogModelValueState;
        ValueState<ReceiptLogModel> receiptLogModelValueState;

        @Override
        public void open(Configuration parameters) throws Exception {

            orderLogModelValueState = getRuntimeContext().getState(new ValueStateDescriptor<OrderLogModel>("orderLogModelValueState", OrderLogModel.class));
            receiptLogModelValueState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptLogModel>("receiptLogModelValueState", ReceiptLogModel.class));
        }

        @Override
        public void processElement1(OrderLogModel pay, CoProcessFunction<OrderLogModel, ReceiptLogModel, OrderTxResult>.Context ctx, Collector<OrderTxResult> out) throws Exception {

            //支付事件来了，去判断是否有对应的到账事件
            ReceiptLogModel receiptLogModel = receiptLogModelValueState.value();
            if (receiptLogModel != null) {
                //如果状态中有对应的到账事件，则说明支付成功
                out.collect(new OrderTxResult(pay.getTxId(), new Timestamp(pay.getTimestamp()).toString(), new Timestamp(receiptLogModel.getTimestamp()).toString(), "processElement1 success"));

                //清空状态
                orderLogModelValueState.clear();
                receiptLogModelValueState.clear();

            } else {

                //如果到账信息没有，则注册一定事时间的定时器，等待到账事件
                ctx.timerService().registerEventTimeTimer((pay.getTimestamp() + 5) * 1000); //5秒钟等待，具体看双流数据延迟
                orderLogModelValueState.update(pay);
            }


        }

        @Override
        public void processElement2(ReceiptLogModel receipt, CoProcessFunction<OrderLogModel, ReceiptLogModel, OrderTxResult>.Context ctx, Collector<OrderTxResult> out) throws Exception {

            //到账事件来了，去判断是否有对应的订单支付事件
            OrderLogModel orderLogModel = orderLogModelValueState.value();
            if (orderLogModel != null) {
                //如果状态中有对应的订单支付事件，则说明支付成功
                out.collect(new OrderTxResult(receipt.getTxId(), new Timestamp(receipt.getTimestamp()).toString(), new Timestamp(orderLogModel.getTimestamp()).toString(), "processElement2 success"));
                //清空状态
                orderLogModelValueState.clear();
                receiptLogModelValueState.clear();
            } else {
                //如果订单支付事件没有，则注册一定事时间的定时器，等待订单支
                ctx.timerService().registerEventTimeTimer((receipt.getTimestamp() + 3) * 1000); //3秒钟等待，具体看双流数据延迟
                receiptLogModelValueState.update(receipt);
            }

        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<OrderLogModel, ReceiptLogModel, OrderTxResult>.OnTimerContext ctx, Collector<OrderTxResult> out) throws Exception {

            //定时器触发

            //1.如果订单支付事件状态不为空，则说明到账信息事件没有来
            OrderLogModel orderLogModel = orderLogModelValueState.value();
            if(orderLogModel != null){
                ctx.output(OUTPUT_TAG_ORDER,orderLogModel);
            }



            //2.如果到账信息事件状态不为空，则说明订单支付事件没有来
            ReceiptLogModel receiptLogModel = receiptLogModelValueState.value();
            if(receiptLogModel != null){
                ctx.output(OUTPUT_TAG_RECEIPT,receiptLogModel);
            }

            //3.清空状态
            orderLogModelValueState.clear();
            receiptLogModelValueState.clear();

        }
    }

}

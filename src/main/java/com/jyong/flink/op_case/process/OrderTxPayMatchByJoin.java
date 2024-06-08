package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.OrderLogModel;
import com.jyong.flink.op_case.beans.OrderTxResult;
import com.jyong.flink.op_case.beans.ReceiptLogModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/6/27 20:27
 * @desc 利用join实现实时对账需求
 */

public class OrderTxPayMatchByJoin {

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
                .filter(s-> StringUtils.isNotBlank(s.getTxId()))
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


        //3.区间连接两条流，得到匹配的数据 TxPayMatchDetect
        SingleOutputStreamOperator<OrderTxResult> result = orderLogDataSource.keyBy(OrderLogModel::getTxId)
                .intervalJoin(receiptLogDataSource.keyBy(ReceiptLogModel::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))
                .process(new TxPayMatchDetect());

        //4.打印输出
        result.print();

        //5.触发
        env.execute();


    }
private static class TxPayMatchDetect extends ProcessJoinFunction<OrderLogModel,ReceiptLogModel,OrderTxResult>{
    @Override
    public void processElement(OrderLogModel left, ReceiptLogModel right, ProcessJoinFunction<OrderLogModel, ReceiptLogModel, OrderTxResult>.Context ctx, Collector<OrderTxResult> out) throws Exception {
        //这里都是成功join匹配到的数据
        out.collect(new OrderTxResult(left.getTxId(),new Timestamp(left.getTimestamp()).toString(),new Timestamp(right.getTimestamp()).toString(),"success match"));


    }
}

}

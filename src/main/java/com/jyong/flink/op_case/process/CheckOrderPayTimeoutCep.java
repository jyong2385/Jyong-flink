package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.OrderLogModel;
import com.jyong.flink.op_case.beans.OrderResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author jyong
 * @Date 2023/6/24 17:31
 * @desc 检测订单超时 利用cep处理
 */

public class CheckOrderPayTimeoutCep {

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
                        return new OrderLogModel(userId,action,tx,timestamp);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderLogModel>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderLogModel>() {
                            @Override
                            public long extractTimestamp(OrderLogModel orderLogModel, long l) {
                                return orderLogModel.getTimestamp();
                            }
                        }));

        //3.定义cep pattern
        Pattern<OrderLogModel, OrderLogModel> payWarnPattern = Pattern.<OrderLogModel>begin("create")
                .where(new SimpleCondition<OrderLogModel>() {
                    @Override
                    public boolean filter(OrderLogModel orderLogModel) throws Exception {
                        return "create".equalsIgnoreCase(orderLogModel.getAction());
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderLogModel>() {
                    @Override
                    public boolean filter(OrderLogModel orderLogModel) throws Exception {
                        return "pay".equalsIgnoreCase(orderLogModel.getAction());
                    }
                }).within(Time.minutes(15));


        //4.定义侧输出流标签，用来表示超时事件
        OutputTag<OrderResult> orderResultOutputTag = new OutputTag<OrderResult>("order-timeout"){};

        //5.将Pattern应用到shu数据流上
        PatternStream<OrderLogModel> patternStream = CEP.pattern(dataSource.keyBy(OrderLogModel::getUserId), payWarnPattern);


        //6.调用select方法，实现对匹配负责事件和超时负责事件
        SingleOutputStreamOperator<OrderResult> selectStream = patternStream.select(orderResultOutputTag, new MyPatternTimeoutFunction(), new MyPatternSelectFunction());

        //7.打印输出
        selectStream.print("payed normally");
        selectStream.getSideOutput(orderResultOutputTag).print("timeout");

        //8.触发执行
        env.execute();

    }

    //自定义超时时间事件处理函数
    private static class MyPatternTimeoutFunction implements PatternTimeoutFunction<OrderLogModel,OrderResult>{
        @Override
        public OrderResult timeout(Map<String, List<OrderLogModel>> map, long timeout) throws Exception {

            OrderLogModel orderLogModel = map.get("create").iterator().next();
            String userId = orderLogModel.getUserId();
            return new OrderResult(userId,"timeout order ,create time: "+new Timestamp(orderLogModel.getTimestamp())+" timeout time:"+new Timestamp(timeout));
        }
    }


    //自定义正常事件处理函数
private static class MyPatternSelectFunction implements PatternSelectFunction<OrderLogModel,OrderResult>{
        @Override
        public OrderResult select(Map<String, List<OrderLogModel>> map) throws Exception {
            OrderLogModel orderLogModelCreate = map.get("create").iterator().next();
            OrderLogModel orderLogModel = map.get("pay").iterator().next();
            return new OrderResult(orderLogModel.getUserId(), "success pay, create time"+new Timestamp(orderLogModelCreate.getTimestamp())+" pay time:"+new Timestamp(orderLogModel.getTimestamp()));
        }
    }

}

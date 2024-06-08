package com.jyong.flink.job.cep;

import com.jyong.flink.entity.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author jyong
 * @Date 2023/5/30 20:59
 * @desc 模式处理延迟数据
 */

public class OrderTimeoutDetectExample {

    public static void main(String[] args) throws Exception {

        //1、创建坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2、数据源
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.fromElements(
                new OrderEvent("user_1", "order_1", "create", 1000L),
                new OrderEvent("user_2", "order_2", "create", 2000L),
                new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent orderEvent, long l) {
                        return orderEvent.getTimestamp();
                    }
                }));



        //3、定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "create".equals(orderEvent.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "pay".equals(orderEvent.getEventType());
                    }
                }).within(Time.minutes(15));

        //3.将模式应用到数据流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(OrderEvent::getUserId), pattern);


        //4.定义侧输出流，用于超时数据处理
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

        //5.将完全匹配和超时部分匹配的复杂事件提取出来，进行处理
        SingleOutputStreamOperator<String> result = patternStream.process(new OrderPayMatch());



        result.print("pay: ");
        DataStream<String> sideOutput = result.getSideOutput(timeoutTag);
        sideOutput.print();

        env.execute();


    }

    //自定义PatternProcessFunction
    public static class OrderPayMatch extends PatternProcessFunction<OrderEvent,String> implements TimedOutPartialMatchHandler<OrderEvent> {

        //处理完全匹配的数据
        @Override
        public void processMatch(Map<String, List<OrderEvent>> map, Context context, Collector<String> collector) throws Exception {
            //获取当前的支付事件
            OrderEvent payEvent = map.get("pay").get(0);
            collector.collect("用户"+payEvent.getUserId()+" 订单 "+payEvent.getOrderId()+" 已支付");
        }

        //处理超时匹配的数据
        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> map, Context context) throws Exception {
            OrderEvent createEvent = map.get("create").get(0);
            OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};
            context.output(timeoutTag,"用户"+createEvent.getUserId()+" 订单 "+createEvent.getOrderId()+" 超时未支付！");

        }
    }

}

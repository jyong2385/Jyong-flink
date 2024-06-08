package com.jyong.flink.job.cep;

import com.jyong.flink.entity.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author jyong
 * @Date 2023/5/30 20:34
 * @desc
 */

public class CepPatternExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //1.获取登陆数据流
        DataStream<LoginEvent> streamSource = env.fromElements(
                new LoginEvent("user_1", "192.168.6.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.6.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.6.2", "fail", 4000L),
                new LoginEvent("user_1", "192.168.6.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.6.23", "success", 6000L),
                new LoginEvent("user_2", "192.168.6.23", "fail", 7000L),
                new LoginEvent("user_2", "192.168.6.23", "fail", 8000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                        return loginEvent.getTimestamp();
                    }
                }));


        //2.定义模式
        Pattern<LoginEvent, LoginEvent> loginEventPattern = Pattern.<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.getEventType().equals("fail");
                    }
                }).times(3).consecutive();

        //3.将模式应用到数据流上，检测复杂事件
        PatternStream<LoginEvent> patternStream = CEP.pattern(streamSource.keyBy(LoginEvent::getUserId), loginEventPattern);

        //4.将检测到的复杂事件提取出来，进行处理的到报警信息输出
        SingleOutputStreamOperator<String> warnStream = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {

                LoginEvent first = map.get("fail").get(0);
                LoginEvent second = map.get("fail").get(1);
                LoginEvent third = map.get("fail").get(2);
                return first.getUserId() + " 连续三次登陆失败！ 登陆时间： " +
                        +first.getTimestamp() + " "
                        + second.getTimestamp() + " "
                        + third.getTimestamp() + " ";
            }
        });

        warnStream.print();

        env.execute();


    }
}

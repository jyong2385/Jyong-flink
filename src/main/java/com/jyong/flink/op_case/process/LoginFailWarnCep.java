package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.UserLoginInfo;
import com.jyong.flink.op_case.beans.UserLoginResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author jyong
 * @Date 2023/6/23 15:24
 * @desc 连续3ds登陆失败告警 pro
 * 由于之前处理方式有漏洞，2秒内的数据存在并发量很高是出现某一个成功，则会导致无法检测出
 * 当出现2次以上的判断时判断比较麻烦。并且当出现乱序数据时，无法判断连续两次时间的数据
 * 对上上述特殊情况，则需求flink提供的负责事件时间处理方式：CEP
 */

public class LoginFailWarnCep {


    public static void main(String[] args) throws Exception {

        //1.坏境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<UserLoginInfo> dataSource = env.readTextFile("/Users/jyong/Desktop/jyong/workplace/coding/Jyong/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/data/LoginLog.csv")
                .map(new MapFunction<String, UserLoginInfo>() {
                    @Override
                    public UserLoginInfo map(String s) throws Exception {
                        /**
                         * //5402,83.149.11.115,success,1558430815
                         */
                        String[] splits = s.split(",");
                        String userId = splits[0];
                        String ip = splits[1];
                        String status = splits[2];
                        Long timestamp = Long.valueOf(splits[3]);
                        return new UserLoginInfo(userId, ip, status, timestamp);
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserLoginInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<UserLoginInfo>() {
                            @Override
                            public long extractTimestamp(UserLoginInfo adClickEvent, long l) {
                                return adClickEvent.getTimestamp() * 1000;
                            }
                        })

                );


        //2。定义cep pattern
        Pattern<UserLoginInfo, UserLoginInfo> loginFailPattern0 = Pattern.<UserLoginInfo>begin("first").where(new SimpleCondition<UserLoginInfo>() {
                    @Override
                    public boolean filter(UserLoginInfo userLoginInfo) throws Exception {
                        return "fail".equalsIgnoreCase(userLoginInfo.getStatus());
                    }
                }).next("second")
                .where(new SimpleCondition<UserLoginInfo>() {
                    @Override
                    public boolean filter(UserLoginInfo userLoginInfo) throws Exception {
                        return "fail".equalsIgnoreCase(userLoginInfo.getStatus());
                    }
                })
                .next("third")
                .where(new SimpleCondition<UserLoginInfo>() {
                    @Override
                    public boolean filter(UserLoginInfo userLoginInfo) throws Exception {
                        return "fail".equalsIgnoreCase(userLoginInfo.getStatus());
                    }
                })
                .within(Time.seconds(5));


        //优化pattern
        Pattern<UserLoginInfo, UserLoginInfo> loginFailPattern1 = Pattern.<UserLoginInfo>begin("failEvent").where(new SimpleCondition<UserLoginInfo>() {
                    @Override
                    public boolean filter(UserLoginInfo userLoginInfo) throws Exception {
                        return "fail".equalsIgnoreCase(userLoginInfo.getStatus());
                    }
                }).times(3).consecutive()  //consecutive严格近邻
                .within(Time.seconds(5));


        //3.利用pattern处理
        SingleOutputStreamOperator<UserLoginResult> result = CEP.pattern(dataSource, loginFailPattern1)
                .select(new PatternSelectFunction<UserLoginInfo, UserLoginResult>() {
                    @Override
                    public UserLoginResult select(Map<String, List<UserLoginInfo>> map) throws Exception {
                        //获取第2次失败的信息

                        //pattern1
//                        UserLoginInfo first = map.get("first").get(0);
//                        UserLoginInfo second = map.get("second").get(0);

                        //pattern2
                        UserLoginInfo first0 = map.get("failEvent").get(0);
                        UserLoginInfo third3 = map.get("failEvent").get(2);

                        return new UserLoginResult(first0.getUserId(), first0.getTimestamp(), third3.getTimestamp(), 3);
                    }
                });
        result.print();

        env.execute();

    }

}

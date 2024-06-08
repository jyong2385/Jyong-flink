package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.UserLoginInfo;
import com.jyong.flink.op_case.beans.UserLoginResult;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @Author jyong
 * @Date 2023/6/23 15:24
 * @desc 连续2s登陆失败告警
 */

public class LoginFailWarn {


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

        //按照用户分组过滤
        SingleOutputStreamOperator<UserLoginResult> process = dataSource.keyBy(UserLoginInfo::getUserId)
                .process(new CustomKeyedProcessFunction(3));

        process.print();

        env.execute();

    }

    private static class CustomKeyedProcessFunction extends KeyedProcessFunction<String, UserLoginInfo, UserLoginResult> {

        private Integer loginCnt;

        public CustomKeyedProcessFunction(Integer loginCnt) {
            this.loginCnt = loginCnt;
        }

        //定义状态记录登陆失败次数
        ListState<UserLoginInfo> userLoginFailInfoListState;
        //定义状态记录是否注册定时器
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {

            userLoginFailInfoListState = getRuntimeContext().getListState(new ListStateDescriptor<UserLoginInfo>("userLoginFailInfoListState", UserLoginInfo.class));

            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState", Long.class));

        }

        @Override
        public void processElement(UserLoginInfo value, KeyedProcessFunction<String, UserLoginInfo, UserLoginResult>.Context ctx, Collector<UserLoginResult> out) throws Exception {

            //判断当前事件登陆类型
            if ("fail".equalsIgnoreCase(value.getStatus())) {

                //如果是登陆失败事件，将数据添加到状态中保存下来
                userLoginFailInfoListState.add(value);

                //如果没有定时器就注册一个2秒后的定时器
                if (timerTsState.value() == null) {
                    Long ts = (value.getTimestamp() + 2) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            } else {
                //登陆成功删除定时器 清除状态
                if (timerTsState.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                }
                userLoginFailInfoListState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, UserLoginInfo, UserLoginResult>.OnTimerContext ctx, Collector<UserLoginResult> out) throws Exception {

            //定时器触发
            //判断是否达到失败次数
            ArrayList<UserLoginInfo> userLoginInfos = Lists.newArrayList(userLoginFailInfoListState.get().iterator());
            int loginFailCnt = userLoginInfos.size();
            if (loginFailCnt >= loginCnt) {
                //触发告警
                out.collect(new UserLoginResult(ctx.getCurrentKey(),userLoginInfos.get(0).getTimestamp(),userLoginInfos.get(userLoginInfos.size()-1).getTimestamp(),loginFailCnt));


            }


            //清空状态
            userLoginFailInfoListState.clear();
            timerTsState.clear();


        }
    }
}

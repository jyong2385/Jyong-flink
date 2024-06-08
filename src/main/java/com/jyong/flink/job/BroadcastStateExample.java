package com.jyong.flink.job;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author jyong
 * @Date 2023/5/18 20:50
 * @desc
 */

public class BroadcastStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //用户的行为数据流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy")
        );

        //行为模式流，基于它构建广播流
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "buy")
        );

        //定义广播状态的描述器，创建广播流
        MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>("map-descriptor", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> bcPatterns = patternStream.broadcast(bcStateDescriptor);

        //联结两条流进行处理
        DataStream<Tuple2<String, Pattern>> matches = actionStream.keyBy(data -> data.userId)
                .connect(bcPatterns)
                .process(new PatternDetector());

        matches.print();

        env.execute();


    }

    //实现自定义的KeyedBroadcastProcessFunction
    public static class PatternDetector extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {

        //定义一个值状态，保存上一次用户行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("prevActionState", Types.STRING));
        }

        @Override
        public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {

            //从广播状态中获取匹配模式
            ReadOnlyBroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<>("map-descriptor", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern = patternState.get(null);

            //获取用户上一次的行为
            String prevAction = prevActionState.value();

            //判断是否匹配
            if (pattern != null && prevAction != null) {
                if (pattern.action1.equals(prevAction) && pattern.action2.equals(value.action))
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
            }

            prevActionState.update(value.action);

        }

        @Override
        public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {

            //从上下文中获取广播状态，并用当前数据更新状态
            BroadcastState<Void, Pattern> pattern = ctx.getBroadcastState(new MapStateDescriptor<>("map-descriptor", Types.VOID, Types.POJO(Pattern.class)));

            pattern.put(null, value);


        }
    }

    //定义用户行为时间和模式的POJO类
    public static class Action {

        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }


    }

    //定义行为模式pojo类，包含先后发生的两个行为
    public static class Pattern {

        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }

}

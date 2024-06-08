package com.jyong.flink.job.operator;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: jyong
 * @description 富函数类
 * @date: 2023/3/26 09:45
 */
public class TransformRichFunction {


    public static void main(String[] args) throws Exception{

        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        //2.从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("zhangsan", "/index", 1000L),
                new Event("lisi", "/cat", 1000L),
                new Event("zhangsan", "/cat", 1000L),
                new Event("zhangsan", "/cat", 2000L),
                new Event("lisi", "/cat", 3000L),
                new Event("tianliu", "/cat", 4000L),
                new Event("wangwu", "/index", 1000L)
        );

        //3.使用自定义的RichFunction富函数类
        SingleOutputStreamOperator<Integer> map = eventDataStreamSource.map(new MyRichFunction());

        map.print();
        //4.触发执行
        env.execute();

    }


    private static class MyRichFunction extends RichMapFunction<Event,Integer> {
        @Override
        public void setRuntimeContext(RuntimeContext t) {
            super.setRuntimeContext(t);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return super.getRuntimeContext();
        }

        @Override
        public IterationRuntimeContext getIterationRuntimeContext() {
            return super.getIterationRuntimeContext();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //创建生命周期的时候首先调用的方法
            System.out.println("open 生命周期方法调用："+ getRuntimeContext().getIndexOfThisSubtask());
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            //销毁生命周期时，最后被调用的方法
            System.out.println("close 生命周期方法调用："+ getRuntimeContext().getIndexOfThisSubtask());
            super.close();
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.getUser().length();
        }
    }
}

package com.jyong.flink.job.operator;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: jyong
 * @description map转换算子
 * @date: 2023/3/22 20:15
 */
public class TransformMap {

    public static void main(String[] args) throws Exception {
        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("zhangsan", "/index", 1000L),
                new Event("lisi", "/cat", 1000L),
                new Event("wangwu", "/index", 1000L)
        );

        //3.进行map转换计算，执行提取user的逻辑
        //m1:匿名内部类
        SingleOutputStreamOperator<String> map = eventDataStreamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.getUser();
            }
        });

        //4.结果打印
        map.print();

        //5.触发任务
        env.execute();
    }
}

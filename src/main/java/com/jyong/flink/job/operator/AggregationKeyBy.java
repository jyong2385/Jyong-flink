package com.jyong.flink.job.operator;

import com.jyong.flink.entity.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: jyong
 * @description 聚合算子KeyBy
 * @date: 2023/3/22 20:34
 */
public class AggregationKeyBy {

    public static void main(String[] args) throws Exception {
        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //2.从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("zhangsan", "/index", 1000L),
                new Event("lisi", "/cat", 1000L),
                new Event("zhangsan", "/cat", 1000L),
                new Event("zhangsan", "/cat", 2000L),
                new Event("lisi", "/cat", 3000L),
                new Event("tianliu", "/cat", 4000L),
                new Event("wangwu", "/index", 1000L)
        );

        //3.按键进行聚合，提取当前用户最近的一次访问数据
        SingleOutputStreamOperator<Event> result = eventDataStreamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.getUser();
            }
        })
                /**
                 * TODO：这里聚合函数中根据字段'timestamp'进行聚合，有可能会出现以下异常：
                 * 原因：数据源是pojo类型，pojo实体类中必须符合标准的pojo类型：
                 * 1. 所有成员变量都是私有的，用private修饰
                 * 2. 每个成员变量都有对应的getter和setter
                 * 3. 有一个全参数的构造方法
                 * 4. 有一个无参的构造方法
                 *Exception in thread "main" org.apache.flink.api.common.typeutils.CompositeType$InvalidFieldReferenceException:
                 * Cannot reference field by field expression on GenericType<com.jyong.flink.entity.Event>
                 *     Field expressions are only supported on POJO types, tuples, and case classes.
                 *     (See the Flink documentation on what is considered a POJO.)
                 */
                .max("timestamp");


        //4.打印
        result.print("max: ");

        //5.触发任务
        env.execute();


    }

}

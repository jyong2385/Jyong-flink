package com.jyong.flink.job.flinksql;

import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author jyong
 * @Date 2023/5/21 11:08
 * @desc
 */

public class SimpleTableExample {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.读取数据
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        //2.创建表执行坏境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);


        //3.将流转换成表
        Table table = tableEnvironment.fromDataStream(dataStream);
        //todo 创建虚拟表
        tableEnvironment.createTemporaryView("click_table",table);
        //查询虚拟表
        tableEnvironment.sqlQuery("select * from click_table");

        //4.直接写sql
        Table query = tableEnvironment.sqlQuery("select user,url from " + table);

        //基于table直接转换
        table.select($("user"),$("url"))
                .where($("user").isEqual("张三"));

        //5.打印输出
        DataStream<Row> rowDataStream = tableEnvironment.toDataStream(query);

        rowDataStream.print("result");


        env.execute();
    }

}

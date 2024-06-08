package com.jyong.flink.op_case.process;

import com.jyong.flink.op_case.beans.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * @Author jyong
 * @Date 2023/6/4 15:50
 * @desc
 */

public class HotItemsWithSql {

    public static void main(String[] args) throws Exception {
        //1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据,创建DataStream数据流
        DataStreamSource<String> inputDataStream = env.readTextFile("/Users/jyong/Desktop/jyong/workplace/coding/Jyong/UserBehaviorAnalysis/data/UserBehavior.csv");

        //3.转换为POJO,分配事件戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputDataStream.map(data -> {
            String[] split = data.split(",");

            return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                        return userBehavior.getTimestamp() * 1000;
                    }
                })
        );

        //4.创建表执行坏境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(dataStream, "userId,itemId,categoryId,behaivor,timestamp.rowtime as ts");
//        Table table = tableEnv.fromDataStream(dataStream, $("userId"), $("itemId"), $("categoryId"), $("behaivor"), $("timestamp").as("ts"));

        //5.分组开窗
        Table tableWindow = table.filter("behaivor == 'pv' ")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId,w")
                .select("itemId, w.end as windowEnd, itemId.count as cnt");

        //6。利用开窗函数，对count值进行排序并获取Row number,的到topn
        tableEnv.createTemporaryView("agg",tableEnv.toAppendStream(tableWindow,Row.class),"itemId,windowEnd,cnt");
        Table result = tableEnv.sqlQuery("select * from (select *, row_number() over(partition by windowEnd order by cnt desc) as row_num from agg) te where row_num <= 5 ");

        tableEnv.createTemporaryView("data_table",table);
        Table table1 = tableEnv.sqlQuery("select * from (select *, row_number() over(partition by windowEnd order by cnt desc) as row_num from " +
                " (select itemId,count(itemId) as cnt ,hop_end(ts,interval '5' minute,interval '1' hour) windowEnd " +
                " from data_table" +
                " where behaivor = 'pv'" +
                " group by itemId,hop(ts,interval '5' minute,interval '1' hour)" +
                ")" +
                ") te where row_num <= 5");

        tableEnv.toRetractStream(table1, Row.class).print();

        env.execute();


    }

}

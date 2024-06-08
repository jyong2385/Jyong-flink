package com.jyong.flink.job.flinksql;

import com.jyong.flink.entity.Event;
import com.jyong.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author jyong
 * @Date 2023/5/21 21:17
 * @desc flink sql定义时间属性的窗口
 */

public class TimeAndWindowTest {


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //2.1创建输入表
        String createDDL = "create table clickTable(" +
                " user_name string," +
                " url string," +
                " ts bigint " +
                " et as to_timestamp (from_unixtime(ts/1000)) ," +
                " watermark for et as et - interval '1' second" +
                ") with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = '/Users/jyong/Desktop/jyong/workplace/coding/Jyong/Jyong-flink/src/main/resources/clicks.txt'," +
                " 'format' = 'csv' " +
                ")";
//        tableEnv.executeSql(createDDL);

        //事件时间
        //2.在流转换成table的时候定义时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimestamp();
                            }
                        })
                );


        Table table = tableEnv.fromDataStream(clickStream, $("user"), $("url"), $("timestamp").as("ts"),
                $("et").rowtime());

        table.printSchema();

        //处理时间
        String createDDL2 = "create table clickTable(" +
                " user_name string," +
                " url string," +
                " ts bigint " +
                " et as proctime () ," +
                ") with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = '/Users/jyong/Desktop/jyong/workplace/coding/Jyong/Jyong-flink/src/main/resources/clicks.txt'," +
                " 'format' = 'csv' " +
                ")";

        /**
         * 分组窗口
         */

        //TUMBLE(ts,INTERVAL '1' HOUR) 定义1小时的 滚动窗口
        String tumbleSql = "select user," +
                " tumble_end(ts,interval '1' hour) as endT," +
                " count(url) cnt" +
                " from eventTable" +
                " group by user,tumble(ts,interval '1' hour)";
        tableEnv.sqlQuery(tumbleSql);

        /**
         * 窗口表值函数 windowing tvfs
         *  滚动窗口（tumbling window）
         *  滑动窗口（hop window,跳跃窗口）
         *  积累窗口（cumulate window）
         *  会话窗口（session window,目前尚未支持）
         */








    }




}

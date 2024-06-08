package com.jyong.flink.job.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author jyong
 * @Date 2023/5/21 13:47
 * @desc
 */

public class CommonSqlApiExample {

    public static void main(String[] args) {

        //1.创建表坏境


        //1.1.利用流式坏境创建
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.2 基于blink计划器定义坏境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                /**
                 * 使用流处理模式
                 */
                .inStreamingMode()
                /**
                 * 计划其默认使用blink
                 */
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //1.3基于老版本planner进行流处理
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .useOldPlanner()
//                .build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //2.创建连接器表

        //2.1创建输入表
        String createDDL = "create table clickTable(" +
                " user_name string," +
                " url string," +
                " ts bigint " +
                ") with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = '/Users/jyong/Desktop/jyong/workplace/coding/Jyong/Jyong-flink/src/main/resources/clicks.txt'," +
                " 'format' = 'csv' " +
                ")";
        tableEnv.executeSql(createDDL);

        //2.1创建输出表
        String createOutDDL = "create table outTable(" +
                " user_name string," +
                " url string" +
                ") with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = '/Users/jyong/Desktop/jyong/workplace/coding/Jyong/Jyong-flink/src/main/resources/output'," +
                " 'format' = 'csv' " +
                ")";

        tableEnv.executeSql(createOutDDL);


        //3.处理
        Table resultTable = tableEnv.from("clickTable").where($("user_name").isEqual("bob"))
                .select($("user_name"), $("url"));





        tableEnv.createTemporaryView("out_Temp_Result_Table",resultTable);

        Table result2 = tableEnv.sqlQuery("select url,user_name from out_Temp_Result_Table");



        //输出表
        //4.1
        // resultTable.executeInsert("outTable");

        //4.2
        result2.executeInsert("outTable");

    }


}

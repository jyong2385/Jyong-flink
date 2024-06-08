package com.jyong.flink.job.flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author jyong
 * @Date 2023/5/21 15:25
 * @desc 创建控制台打印的table
 */

public class PrintTableExample {

    public static void main(String[] args) {

        //1 基于blink计划器定义坏境
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

        //3.todo 创建输出打印表
        String printOutDDL = "create table printOutTable(" +
                " user_name string," +
                " url string," +
                " ts bigint " +
                ") with (" +
                " 'connector' = 'print'" +
                ")";
        tableEnv.executeSql(printOutDDL);

        tableEnv.from("clickTable")
                .executeInsert("printOutTable");


    }
}

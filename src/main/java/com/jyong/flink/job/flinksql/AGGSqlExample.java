package com.jyong.flink.job.flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author jyong
 * @Date 2023/5/21 15:30
 * @desc
 */

public class AGGSqlExample {


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
        Table clickTable = tableEnv.from("clickTable");

        //注册虚拟表
        tableEnv.createTemporaryView("click_table",clickTable);

        //执行聚合查询
        Table sqlQueryResult = tableEnv.sqlQuery("select user_name,count(url) as cnt from click_table group by user_name");

        //创建输出打印表
        String printOutDDL = "create table printOutTable(" +
                " user_name string," +
                " cnt bigint " +
                ") with (" +
                " 'connector' = 'print'" +
                ")";
        tableEnv.executeSql(printOutDDL);

        sqlQueryResult.executeInsert("printOutTable");







    }
}

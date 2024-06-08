package com.jyong.flink.job.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author jyong
 * @Date 2023/5/27 14:03
 * @desc uv中的top2案例
 */

public class TopNExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //1.在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable( " +
                " `user` string, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000)), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = '/Users/jyong/Desktop/jyong/workplace/coding/Jyong/Jyong-flink/src/main/resources/clicks.txt', " +
                " 'format' = 'csv' " +
                " )";
        tableEnv.executeSql(createDDL);

        //窗口TopN,统计一段时间内的前2名活跃用户

        String top2SubSql = "SELECT user,count(url) as cnt,window_start,window_end " +
                "FROM TABLE( " +
                " TUMBLE(TABLE clickTable,DESCRIPTOR(et),INTERVAL '10' SECOND) " +
                ") " +
                "GROUP BY user,window_start,window_end ";

        String top2Sql = "SELECT user,cnt,row_num " +
                "FROM ( " +
                "  SELECT *,ROW_NUMBER() OVER( " +
                "    PARTITION BY window_start,window_end " +
                "    ORDER BY cnt desc) as row_num " +
                "  FROM ( "+top2SubSql+" ) a " +
                ") b WHERE row_num <=2 ";

        System.out.println(top2Sql);

        Table top2Table = tableEnv.sqlQuery(top2Sql);


        tableEnv.toDataStream(top2Table)
                        .print("window top n: ");



        env. execute();


    }


}

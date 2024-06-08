package com.jyong.flink.sink;

import com.jyong.flink.entity.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author: jyong
 * @description 写入到mysql
 * @date: 2023/3/27 20:35
 */
public class SinkToJDBC {

    public static void main(String[] args) throws Exception {


        //1.创建流式执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //2.从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("zhangsan", "/index", 1000L), new Event("lisi", "/cat", 1000L), new Event("zhangsan", "/cat", 1000L), new Event("zhangsan", "/cat", 2000L), new Event("lisi", "/cat", 3000L), new Event("tianliu", "/cat", 4000L), new Event("wangwu", "/index", 1000L));

        //3.创建sink
        JdbcConnectionOptions connectorOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:mysql://localhost:3306/work_db").withUsername("root").withPassword("123456").withDriverName("com.mysql.jdbc.Driver").build();

        JdbcStatementBuilder<Event> jdbcStatementBuilder = new JdbcStatementBuilder<Event>() {
            @Override
            public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                preparedStatement.setString(1, event.getUser());
                preparedStatement.setString(2, event.getUrl());
            }
        };

        String operator_sql = "insert into clicks.txt(user,url) values(?,?)";
        eventDataStreamSource.addSink(JdbcSink.sink(operator_sql, jdbcStatementBuilder, connectorOptions));

        //4.触发
        env.execute();


    }


}

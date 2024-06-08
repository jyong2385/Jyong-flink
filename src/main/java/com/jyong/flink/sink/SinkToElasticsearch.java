package com.jyong.flink.sink;

import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.jyong.flink.entity.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: jyong
 * @description 输出到es
 * @date: 2023/3/27 20:18
 */
public class SinkToElasticsearch {

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

        //3.接入 es sink
        //3.1 定义hosts列表
        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("localhost", 9200));

        //3.3 注册sink
        eventDataStreamSource.addSink(new ElasticsearchSink.Builder<>(hosts, new MyElasticsearchSink()).build());

        //4.触发
        env.execute();

    }


    //3.2 定义ElasticsearchSinkFunction
    private static class MyElasticsearchSink implements ElasticsearchSinkFunction<Event> {
        //索引名称
        String indexName = "flink_index_name";

        @Override
        public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

            IndexRequest indexRequest = Requests.indexRequest()
                    .index(indexName)
                    .create(true)
                    .source(JSONUtil.toJsonStr(event));

            requestIndexer.add(indexRequest);


        }
    }
}

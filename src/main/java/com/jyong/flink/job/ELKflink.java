package com.jyong.flink.job;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jyong.flink.entity.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jyong on 2020/11/28 11:52
 */
public class ELKflink {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        properties.setProperty("zookeeper.connect", "node01:2181,node02:2181,node03:2181");
        properties.setProperty("group.id", "ELK_GROUP");
        properties.setProperty("auto.offset.reset", "latest");

        //创建env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //添加source
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("mysql_topic_person", new SimpleStringSchema(), properties));


        SingleOutputStreamOperator<Person> outputStreamOperator = dataStreamSource.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String s) throws Exception {
                JSONObject jsonObject = JSONUtil.parseObj(s);
                Person person = new Person(jsonObject.getStr("id"),
                        jsonObject.getStr("name"),
                        jsonObject.getStr("sex"),
                        jsonObject.getStr("birthday"),
                        jsonObject.getStr("address"),
                        jsonObject.getStr("amount"),
                        jsonObject.getStr("education"),
                        jsonObject.getStr("job"),
                        jsonObject.getStr("phone"),
                        jsonObject.getStr("creditcard"));
                return person;
            }
        });

        Map<String, String> map = Maps.newHashMap();
        map.put("cluster.name", "my-application");
        List<HttpHost> list = Lists.newArrayList();
        list.add(new HttpHost("node01", 9200, "http"));
        list.add(new HttpHost("node02", 9200, "http"));
        list.add(new HttpHost("node03", 9200, "http"));
        //添加sink
        ElasticsearchSink.Builder<Person> personBuilder = new ElasticsearchSink.Builder<Person>(list, new ElasticsearchSinkFunction<Person>() {
            @Override
            public void process(Person element, RuntimeContext ctx, RequestIndexer indexer) {
                String dt = DateTime.now().toString("yyyyMMdd");
                if (element != null) {
                    IndexRequest source = Requests.indexRequest()
                            .index("elk-person-" + dt)
                            .id(element.getId())
                            .source(JSONUtil.toJsonStr(element), XContentType.JSON);
                    indexer.add(source);
                }
            }
        });
// 设置批量写数据的缓冲区大小
        personBuilder.setBulkFlushMaxActions(1);
//        Header[] defaultHeaders = new Header[]{new BasicHeader("Authorization", Http)};
//        personBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders(defaultHeaders);
//                });
        outputStreamOperator.addSink(personBuilder.build());
        env.execute();
    }
}

package com.jyong.flink.datafactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MockKafkaProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "dataompro03.test.bbdops.com:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {

        }

        int n = 0;
        while (n < 1000) {
            Thread.sleep(1000);
            System.out.println("-----数据发送---->"+n);
            producer.send(new ProducerRecord<String, String>("wjy-test-topic", Integer.toString(n), Integer.toString(n)));
            n++;
        }
        producer.close();
    }

}

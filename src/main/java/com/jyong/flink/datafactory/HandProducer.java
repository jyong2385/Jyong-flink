package com.jyong.flink.datafactory;


import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author ：intsmaze
 * @date ：Created in 2020/2/6 12:16
 * @description： https://www.cnblogs.com/intsmaze/
 * @modified By：
 */
public class HandProducer {

    public static String readFile() {
        InputStream path = HandProducer.class.getResourceAsStream("D:\\codding\\myjob\\src\\main\\resources\\words");
        BufferedReader reader = new BufferedReader(new InputStreamReader(path));
        StringBuffer sbf = new StringBuffer();
        try {
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                sbf.append(tempStr);
            }
            reader.close();
            return sbf.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return sbf.toString();
    }

    /**
     * @author intsmaze
     * @description: https://www.cnblogs.com/intsmaze/
     *
     * 要求将sendMess成功发送到topic中，且topic的该消息被消费成功
     * @date : 2020/2/5 12:15
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

        String sendMess="";
        for(int i=1;i<100;i++)
        {
            sendMess= StringUtils.join(sendMess,HandProducer.readFile());

        }


        producer.close();
    }
}

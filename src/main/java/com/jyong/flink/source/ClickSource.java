package com.jyong.flink.source;

import com.jyong.flink.entity.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author: jyong
 * @description 自定义source，点击事件
 * @date: 2023/3/20 20:43
 */
public class ClickSource implements SourceFunction<Event> {

    //开关
    private boolean flag = true;


    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        //随机生成数据
        Random random = new Random();

        //伪造随机数据
        String[] usernames = {"刘一", "陈二", "张三", "李四", "王五", "赵六", "田七", "孙八", "吴九", "郑十"};
        String[] urls = {"/index.html", "/home.html", "/product", "/cart"};

        while (flag) {
            String username = usernames[random.nextInt(usernames.length)];
            String url = urls[random.nextInt(urls.length)];
            Event event = new Event(username, url, System.currentTimeMillis());
            sourceContext.collect(event);
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        this.flag = false;
    }
}

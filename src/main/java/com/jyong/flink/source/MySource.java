package com.jyong.flink.source;

import cn.hutool.core.util.RandomUtil;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jyong on 2021/1/10 17:57
 */
public class MySource implements SourceFunction<String> {

    private Logger logger = LoggerFactory.getLogger(MySource.class);

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {


        while (true) {
            String s = RandomUtil.randomString(10);
            sourceContext.collect(s);
        }
    }

    @Override
    public void cancel() {
        logger.warn("==============已取消source数据=============");
    }
}

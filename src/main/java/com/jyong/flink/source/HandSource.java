package com.jyong.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ：intsmaze
 * @date ：Created in 2020/2/12 10:45
 * @description： https://www.cnblogs.com/intsmaze/
 * @modified By：
 */
public class HandSource implements SourceFunction<String> {

    public static Logger LOG = LoggerFactory.getLogger(HandSource.class);

    public void run(SourceContext<String> ctx) throws Exception {

        for(int i=1;i<100;i++)
        {
            ctx.collect(i+"");
            Thread.sleep(1000);
        }

    }

    public void cancel() {
        LOG.info("---》》》》》》》》》》{}","取消任务");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);//设置了全局的并行度，但是对实现SourceFunction的数据源仍然无效，实现SourceFunction的数据源的默认
        //并行度是1，要想观察到source的并行度不设置为1，抛出异常，只能在addSource(new HandSource())后面针对这个算子设置并行度，才会使得并行度的设置生效

        DataStreamSource<String> stringDataStreamSource = env.addSource(new HandSource());
        //Flink中所有流数据源的基本接口。 当源应开始发射元素时，run方法将被调用，方法使用SourceContext发射元素。源必须通过中断其run中的循环来响应cancel()的调用。
        //这个source的并行度还只能设置为1，如果不设置为1，还会抛出如下错误：

        stringDataStreamSource.print("自定义数据源:");
        env.execute();

    }

}

package com.jyong.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Auther: wangjunyong
 * @Date: 2021/2/4 17:48
 * @Description:写入HBase * 第一种：继承RichSinkFunction重写父类方法
 * *
 * * 注意：由于flink是一条一条的处理数据，所以我们在插入hbase的时候不能来一条flush下，
 * * 不然会给hbase造成很大的压力，而且会产生很多线程导致集群崩溃，所以线上任务必须控制flush的频率。
 * *
 * * 解决方案：我们可以在open方法中定义一个变量，然后在写入hbase时比如500条flush一次，或者加入一个list，判断list的大小满足某个阀值flush一
 */
public class HbaseWriter extends RichSinkFunction<String> {


    Connection conn = null;
    Scan scan = null;
    BufferedMutator mutator = null;
    Integer count = 0;

    /**
     * 建立HBase连接
     *
     * @param parameters
     * @throws IOException
     */
    public void open(Configuration parameters) throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, "10.28.102.158");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        conn = ConnectionFactory.createConnection(config);

        TableName tableName = TableName.valueOf("test");
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        params.writeBufferSize(1024 * 1024); //设置缓存的大小
        mutator = conn.getBufferedMutator(params);
        count = 0;
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        String cf1 = "cf1";
        String[] array = value.split(",");
        Put put = new Put(Bytes.toBytes(array[0]));
        put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array[1]));
        put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array[2]));
        mutator.mutate(put);
        //每满2000条刷新一下数据
        if (count >= 2000) {
            mutator.flush();
            count = 0;
        }
        count = count + 1;
    }

    public void close() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

package com.jyong.flink.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.Iterator;

/**
 * @Auther: wangjunyong
 * @Date: 2021/2/4 17:31
 * @Description: 以HBase为数据源
 * * 从HBase中获取数据，然后以流的形式发送
 * *
 * * 从HBase读取数据
 * * 第一种：继承RichSourceFunction重写父类方法
 */
public class HbaseReader extends RichSourceFunction<String> {

    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;

    public void open(Configuration configuration) throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        TableName tableName = TableName.valueOf("test");
        String cf1 = "cf1";
        conn = ConnectionFactory.createConnection(config);
        table = conn.getTable(tableName);
        scan = new Scan();
        scan.setStartRow(Bytes.toBytes("100"));
        scan.setStopRow(Bytes.toBytes("107"));
        scan.addFamily(Bytes.toBytes(cf1));
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String s = Bytes.toString(result.getRow());
            StringBuilder sb = new StringBuilder(s);
            for (Cell cell : result.listCells()) {
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                sb.append(value).append("_");
            }
            String str = sb.replace(sb.length() - 1, sb.length(), "").toString();
            ctx.collect(str);
        }
    }

    @Override
    public void cancel() {

    }

    public void colseConn() {
        try {
            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}

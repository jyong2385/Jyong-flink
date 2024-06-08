package com.jyong.flink.op_case.beans;

import java.sql.Timestamp;

/**
 * @Author jyong
 * @Date 2023/6/26 20:43
 * @desc
 */

public class ReceiptLogModel {


    //329d09f9f,alipay,1558430893
    private String txId;
    private String channel;
    private Long timestamp;

    public ReceiptLogModel() {
    }

    public ReceiptLogModel(String txId, String channel, Long timestamp) {
        this.txId = txId;
        this.channel = channel;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ReceiptLogModel{" +
                "txId='" + txId + '\'' +
                ", channel='" + channel + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}

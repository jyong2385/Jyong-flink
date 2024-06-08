package com.jyong.flink.op_case.beans;

import java.sql.Timestamp;

/**
 * @Author jyong
 * @Date 2023/6/24 17:25
 * @desc
 */

public class OrderLogModel {

    //34747,pay,329d09f9f,1558430893
    private String userId;
    private String action;

    private String txId;
    private Long timestamp;

    public OrderLogModel() {
    }

    public OrderLogModel(String userId, String action, String txId, Long timestamp) {
        this.userId = userId;
        this.action = action;
        this.txId = txId;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderLogModel{" +
                "userId='" + userId + '\'' +
                ", action='" + action + '\'' +
                ", txId='" + txId + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Long getTimestamp() {
        return timestamp;
    }


    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}

package com.jyong.flink.op_case.beans;

/**
 * @Author jyong
 * @Date 2023/6/26 21:03
 * @desc
 */

public class OrderTxResult {

    private String txId;

    private String orderTime;

    private String receiptTime;

    private String msg;

    public OrderTxResult() {
    }

    public OrderTxResult(String txId, String orderTime, String receiptTime, String msg) {
        this.txId = txId;
        this.orderTime = orderTime;
        this.receiptTime = receiptTime;
        this.msg = msg;
    }

    public String getTxId() {
        return txId;
    }

    @Override
    public String toString() {
        return "OrderTxResult{" +
                "txId='" + txId + '\'' +
                ", orderTime='" + orderTime + '\'' +
                ", receiptTime='" + receiptTime + '\'' +
                ", msg='" + msg + '\'' +
                '}';
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    public String getReceiptTime() {
        return receiptTime;
    }

    public void setReceiptTime(String receiptTime) {
        this.receiptTime = receiptTime;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}

package com.jyong.flink.op_case.beans;

/**
 * @Author jyong
 * @Date 2023/6/24 17:29
 * @desc
 */

public class OrderResult {

    private String userId;
    private String resultState;

    public OrderResult() {
    }

    public OrderResult(String userId, String resultState) {
        this.userId = userId;
        this.resultState = resultState;
    }

    @Override
    public String toString() {
        return "OderResult{" +
                "userId='" + userId + '\'' +
                ", resultState='" + resultState + '\'' +
                '}';
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getResultState() {
        return resultState;
    }

    public void setResultState(String resultState) {
        this.resultState = resultState;
    }
}

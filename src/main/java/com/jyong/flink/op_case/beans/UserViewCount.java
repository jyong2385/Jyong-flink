package com.jyong.flink.op_case.beans;

import java.sql.Timestamp;

/**
 * @Author jyong
 * @Date 2023/6/11 12:32
 * @desc
 */

public class UserViewCount {

    private String userId;
    private Long windowEnd;
    private Long count;

    public UserViewCount() {
    }

    public UserViewCount(String userId, Long windowEnd, Long count) {
        this.userId = userId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "UserViewCount{" +
                "userId='" + userId + '\'' +
                ", windowEnd=" + new Timestamp(windowEnd) +
                ", count=" + count +
                '}';
    }
}

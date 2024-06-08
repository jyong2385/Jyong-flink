package com.jyong.flink.op_case.beans;

/**
 * @Author jyong
 * @Date 2023/6/23 15:26
 * @desc
 */

public class UserLoginResult {

    private String userId;
    private Long firstLoginTime;
    private Long lastLoginTime;
    private Integer times;

    public UserLoginResult() {
    }


    public UserLoginResult(String userId, Long firstLoginTime, Long lastLoginTime, Integer times) {
        this.userId = userId;
        this.firstLoginTime = firstLoginTime;
        this.lastLoginTime = lastLoginTime;
        this.times = times;
    }

    public void setFirstLoginTime(Long firstLoginTime) {
        this.firstLoginTime = firstLoginTime;
    }


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getFirstLoginTime() {
        return firstLoginTime;
    }

    public Long getLastLoginTime() {
        return lastLoginTime;
    }

    public void setLastLoginTime(Long lastLoginTime) {
        this.lastLoginTime = lastLoginTime;
    }

    public Integer getTimes() {
        return times;
    }

    public void setTimes(Integer times) {
        this.times = times;
    }

    @Override
    public String toString() {
        return "UserLoginResult{" +
                "userId='" + userId + '\'' +
                ", 第一次登陆时间=" + firstLoginTime +
                ", 最后一次登陆时间=" + lastLoginTime +
                ", 失败次数=" + times +
                '}';
    }
}

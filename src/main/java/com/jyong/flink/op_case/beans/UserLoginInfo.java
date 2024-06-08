package com.jyong.flink.op_case.beans;

/**
 * @Author jyong
 * @Date 2023/6/23 15:25
 * @desc
 */

public class UserLoginInfo {


    //5402,83.149.11.115,success,1558430815

    private String userId;
    private String ip;
    private String status;
    private Long timestamp;

    public UserLoginInfo() {
    }

    public UserLoginInfo(String userId, String ip, String status, Long timestamp) {
        this.userId = userId;
        this.ip = ip;
        this.status = status;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserLoginInfo{" +
                "userId='" + userId + '\'' +
                ", ip='" + ip + '\'' +
                ", status='" + status + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}

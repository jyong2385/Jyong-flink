package com.jyong.flink.op_case.beans;

import java.sql.Timestamp;

/**
 * @Author jyong
 * @Date 2023/6/6 19:49
 * @desc
 */

public class ApacheLogEvent {
    /**
     *
     * 83.149.9.216 - - 17/05/2015:10:05:07 +0000 GET /presentations/logstash-monitorama-2013/plugin/notes/notes.js
     */

    private String ip;
    private String userId;
    private String url;
    private String method;
    private Long timestamp;

    public ApacheLogEvent() {
    }

    public ApacheLogEvent(String ip, String userId, String url, String method, Long timestamp) {
        this.ip = ip;
        this.userId = userId;
        this.url = url;
        this.method = method;
        this.timestamp = timestamp;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", url='" + url + '\'' +
                ", method='" + method + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}

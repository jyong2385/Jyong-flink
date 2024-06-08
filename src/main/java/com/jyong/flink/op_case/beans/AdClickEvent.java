package com.jyong.flink.op_case.beans;

import java.sql.Timestamp;

/**
 * @Author jyong
 * @Date 2023/6/22 21:21
 * @desc
 */

public class AdClickEvent {

    /**
     * 937166,1715,beijing,beijing,1511661602
     */

    private String userId;
    private String adId;
    private String province;
    private String city;
    private Long timestamp;

    public AdClickEvent() {
    }

    public AdClickEvent(String userId, String adId, String province, String city, Long timestamp) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAdId() {
        return adId;
    }

    public void setAdId(String adId) {
        this.adId = adId;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AdClickEvent{" +
                "userId='" + userId + '\'' +
                ", adId='" + adId + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}

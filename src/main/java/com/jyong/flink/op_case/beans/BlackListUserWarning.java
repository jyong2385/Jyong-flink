package com.jyong.flink.op_case.beans;

/**
 * @Author jyong
 * @Date 2023/6/23 10:13
 * @desc
 */


public class BlackListUserWarning {

    private String userId;
    private String adId;
    private String warningMsg;

    public BlackListUserWarning() {
    }

    public BlackListUserWarning(String userId, String adId, String warningMsg) {
        this.userId = userId;
        this.adId = adId;
        this.warningMsg = warningMsg;
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

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }

    @Override
    public String toString() {
        return "BlackListUserWarning{" +
                "userId='" + userId + '\'' +
                ", adId='" + adId + '\'' +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }
}

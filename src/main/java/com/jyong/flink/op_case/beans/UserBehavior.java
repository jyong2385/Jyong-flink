package com.jyong.flink.op_case.beans;

/**
 * @Author jyong
 * @Date 2023/5/31 20:49
 * @desc 定义用户行为属性
 */

public class UserBehavior {

    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behaivor;
    private Long timestamp;

    public UserBehavior() {
    }

    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behaivor, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behaivor = behaivor;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehaivor() {
        return behaivor;
    }

    public void setBehaivor(String behaivor) {
        this.behaivor = behaivor;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behaivor='" + behaivor + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}

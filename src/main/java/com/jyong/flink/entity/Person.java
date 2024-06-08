package com.jyong.flink.entity;

/**
 * Created by jyong on 2021/1/23 16:28
 */
public class Person {

    private String id;
    private String name;
    private String sex;
    private String birthday;
    private String address;
    private String amount;
    private String education;
    private String job;
    private String idphone;
    private String creditcard;

    public Person(String id, String name, String sex, String birthday, String address, String amount, String education, String job, String idphone, String creditcard) {
        this.id = id;
        this.name = name;
        this.sex = sex;
        this.birthday = birthday;
        this.address = address;
        this.amount = amount;
        this.education = education;
        this.job = job;
        this.idphone = idphone;
        this.creditcard = creditcard;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getEducation() {
        return education;
    }

    public void setEducation(String education) {
        this.education = education;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getIdphone() {
        return idphone;
    }

    public void setIdphone(String idphone) {
        this.idphone = idphone;
    }

    public String getCreditcard() {
        return creditcard;
    }

    public void setCreditcard(String creditcard) {
        this.creditcard = creditcard;
    }
}

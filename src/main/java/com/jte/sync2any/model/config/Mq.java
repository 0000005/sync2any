package com.jte.sync2any.model.config;

import lombok.Data;

@Data
public class Mq {
    /**
     * 从哪个topicName读取消息
     */
    private String topicName;
    /**
     * 消费者使用的topicGroup。每次重启本应用都会从kafka的"earliest"处开始读取。
     */
    private String topicGroup;
    /**
     * mq的用户名
     */
    private String username;
    /**
     * mq的密码
     */
    private String password;
}

package com.jte.sync2any.model.config;

import lombok.Data;

import java.util.Random;

@Data
public class Mq {
    /**
     * 从哪个topicName读取消息
     */
    private String topicName;
    /**
     * 【选填】消费者使用的topicGroup，如果不填写，则随机生成。每次重启本应用都会从kafka的"earliest"处开始读取。
     */
    private String topicGroup="topic_group_"+new Random().nextInt(999999);
    /**
     * mq的用户名
     */
    private String username;
    /**
     * mq的密码
     */
    private String password;
}

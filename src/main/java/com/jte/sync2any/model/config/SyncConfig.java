package com.jte.sync2any.model.config;

import lombok.Data;

import java.util.List;

@Data
public class SyncConfig {
    private Mq mq;
    private String dbName;
    /**
     * 要同步的表，多个表用逗号分隔，支持正则表达式
     */
    private String syncTables;
    /**
     * 【选填】针对syncTables匹配到的表去自定义一些规则
     */
    private List<Rule> rules;
    /**
     * 延迟告警(秒)
     */
    private int maxDelayInSecond=-1;
    /**
     * 空闲告警(分钟)
     */
    private int maxIdleInMinute=-1;
    /**
     * 下次告警时间(分钟)
     */
    private int nextTriggerAlertInMinute=60*24;
}

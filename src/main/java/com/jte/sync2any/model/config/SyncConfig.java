package com.jte.sync2any.model.config;

import com.jte.sync2any.core.Constants;
import lombok.Data;

import java.util.List;

@Data
public class SyncConfig {
    private Mq mq;
    /**
     * 同步目的地的类型【es/mysql】
     */
    private String targetType;
    /**
     * 待同步的源数据库ID
     */
    private String sourceDbId;
    /**
     * 同步到的目标源数据库ID
     */
    private String targetDbId;
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
    private int maxDelayInSecond = -1;
    /**
     * 空闲告警(分钟)
     */
    private int maxIdleInMinute = -1;
    /**
     * 下次告警时间(分钟)
     */
    private int nextTriggerAlertInMinute = 60 * 24;
    /**
     * 是否载入原始数据
     */
    private String dumpOriginData = Constants.YES;
}

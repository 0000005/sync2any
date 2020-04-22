package com.jte.sync2es.model.mq;

import lombok.Data;

import java.util.List;

/**
 * 腾讯MQ的消息
 */
@Data
public class TcMqMessage {
    public static String NULL_STR="'##isnull##'";
    private String logtype;
    private int eventtype;
    private String eventtypestr;
    private String db;
    private String table;
    private String localip;
    private int localport;
    private long begintime;
    private String gtid;
    private String serverid;
    private String event_index;
    private String xid;
    private String sql;
    /**
     * 当为update、delete语句时有值
     */
    private List<String> where;
    /**
     * 当为insert、update语句时有值
     */
    private List<String> field;
}

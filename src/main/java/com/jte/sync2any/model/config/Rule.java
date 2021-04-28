package com.jte.sync2any.model.config;

import lombok.Data;

/**
 * 规则配置
 */
@Data
public class Rule {
    /**
     * 表名，支持正则表达式
     */
    private String table;
    /**
     * 同步到目标数据源的表名或index（对es来说）
     */
    private String indexTable;
    /**
     * 【目标数据库为mysql时不支持】自定义同步到es的字段名称和字段类型
     * 字段类型请参考类：com.jte.sync2any.model.es.EsDateType
     * 如：map:  {"group_code":"groupCode","hotel_code":"hotelCode,integer","user_code":",integer"}
     */
    private String map;
    /**
     * 字段过滤，多个字段用逗号分隔。如果有值，则只保留这里填写的字段。
     */
    private String fieldFilter;
    /**
     * 【可选】分表计算器
     */
    private String dynamicTablenameAssigner;
    /**
     * 【可选】分区健
     */
    private String shardingKey;


}

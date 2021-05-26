package com.jte.sync2any.model.es;

import com.jte.sync2any.model.mq.SubscribeDataProto;
import com.jte.sync2any.model.mysql.Field;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.model.mysql.TableRecords;
import lombok.Data;

import java.util.Map;

/**
 * 封装增删改的请求
 */
@Data
public class CudRequest {
    /**
     * 操作类型
     */
    private SubscribeDataProto.DMLType dmlType;

    /**
     * 对于目标数据源：对mysql来说是表名，对es来说是index名称
     */
    private String table;

    /**
     * 主键以字符串的形式保存，联合主键时以“_”拼接（在es中作为主键）
     */
    private String pkValueStr;

    /**
     * 主键，key为列名称，value为主键值
     */
    private Map<String, Field> pkValueMap;

    /**
     * row data,会根据sql的操作类型不同而取TableRecords中不同位置的数据
     */
    private Map<String, Object> parameters;

    private TableMeta tableMeta;
    /**
     * 父级
     */
    private TableRecords records;
}

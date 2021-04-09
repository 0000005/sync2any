package com.jte.sync2any.model.es;

import com.jte.sync2any.model.mysql.Field;
import com.jte.sync2any.model.mysql.TableMeta;
import lombok.Data;

import java.util.Map;

/**
 * 封装增删改的请求
 */
@Data
public class CudRequest {
    /**
     * 操作类型
     * Also see: com.jte.sync2any.extract.impl.KafkaMsgListener
     */
    private String operationType;

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
     * row data
     */
    private Map<String, Object> parameters;

    private TableMeta tableMeta;
}

package com.jte.sync2es.extract;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.jte.sync2es.model.mysql.ColumnMeta;
import com.jte.sync2es.model.mysql.TableMeta;

import java.util.List;

/**
 * 规则解析器：
 * 1、哪些表需要被传输
 * 2、表的字段名称和字段类型的映射
 * 3、索引名称的映射
 * 4、过滤器的规则
 */
public interface RuleParser {
    /**
     * key: dbId$tableName
     * value: TableMeta
     */
    Cache<String, TableMeta> RULES_MAP = CacheBuilder.newBuilder().build();
    void initRules();
}

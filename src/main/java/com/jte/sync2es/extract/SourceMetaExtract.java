package com.jte.sync2es.extract;

import com.jte.sync2es.model.mysql.TableMeta;

import java.util.List;

public interface SourceMetaExtract {
    TableMeta getTableMate(String dbName,String tableName);

    /**
     * 获取源数据的行数
     * @param dbName
     * @param tableName
     * @return
     */
    Long getDataCount(String dbName,String tableName);

    /**
     *  获取表名
     * @param dbName
     * @return
     */
    List<String> getAllTableName(String dbName);
}

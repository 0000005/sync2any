package com.jte.sync2any.extract;

import com.jte.sync2any.model.mysql.TableMeta;

import java.util.List;

public interface SourceMetaExtract {
    TableMeta getTableMate(String dbId,String tableName);

    /**
     * 获取源数据的行数
     * @param dbId
     * @param tableName
     * @return
     */
    Long getDataCount(String dbId,String tableName);

    /**
     *  获取该数据库的所有表名
     * @param dbId
     * @return
     */
    List<String> getAllTableName(String dbId);
}

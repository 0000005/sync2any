package com.jte.sync2es.extract;

import com.jte.sync2es.model.mysql.TableMeta;

import java.util.List;

public interface SourceExtract {
    TableMeta getTableMate(String dbId,String tableName);
    List<String> getAllTableName(String dbId);
}

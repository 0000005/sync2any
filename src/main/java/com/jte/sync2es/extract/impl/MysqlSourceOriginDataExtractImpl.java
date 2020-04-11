package com.jte.sync2es.extract.impl;

import com.jte.sync2es.extract.SourceOriginDataExtract;
import com.jte.sync2es.model.mysql.TableMeta;

import java.io.File;

/**
 * 从mysql中提取原始数据
 */
public class MysqlSourceOriginDataExtractImpl implements SourceOriginDataExtract {
    @Override
    public File dumpData(TableMeta tableMeta) {
        return null;
    }
}

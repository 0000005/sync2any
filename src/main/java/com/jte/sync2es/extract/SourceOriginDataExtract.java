package com.jte.sync2es.extract;

import com.jte.sync2es.model.mysql.TableMeta;

import java.io.File;

/**
 * 提取源中的初始数据
 * 先提取源数据，再去追kafka中的记录。
 */
public interface SourceOriginDataExtract {
    /**
     *
     * @param tableMeta
     * @return dump出来的数据文件地址
     */
    File dumpData(TableMeta tableMeta);
}

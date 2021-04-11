package com.jte.sync2any.transform;

import com.jte.sync2any.model.mysql.TableMeta;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;

/**
 * 将从mysql导出的dumpfile文件转化为CudRequest
 */
public interface DumpTransform {
    /**
     *
     * @param file 数据文件地址
     * @param tableMeta 表的元信息
     *
     * @return
     */
    Iterator transform(File file, TableMeta tableMeta) throws FileNotFoundException;
}

package com.jte.sync2any.transform;

import com.jte.sync2any.model.mysql.TableMeta;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;

/**
 * dump原始数据
 *
 */
public interface DumpTransform {
    /**
     * 将mysqldump出来的信息解析成为可操控的es对象
     * @param file 数据文件地址
     * @param tableMeta 表的元信息
     *
     * @return
     */
    Iterator transform(File file, TableMeta tableMeta) throws FileNotFoundException;
}

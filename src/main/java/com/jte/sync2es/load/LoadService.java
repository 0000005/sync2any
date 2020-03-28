package com.jte.sync2es.load;

import com.jte.sync2es.model.es.EsRequest;
import com.jte.sync2es.model.mysql.TableRecords;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public interface LoadService {
    /**
     * 用于保存已经加载过的load对象。
     * 对es来说就是index
     * 命名规则：Load类型$dbName$tableName   如：“es_mall_user”
     */
    Set<String> LOAD_STORAGE = new HashSet<>();

    int operateData(EsRequest request) throws IOException;
    int addData(EsRequest request) throws IOException;
    int deleteData(EsRequest request) throws IOException;
    int updateData(EsRequest request) throws IOException;
    void checkAndCreateStorage(EsRequest request) throws IOException;
}

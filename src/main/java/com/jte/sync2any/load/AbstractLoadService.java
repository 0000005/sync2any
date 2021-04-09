package com.jte.sync2any.load;

import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.TableMeta;

import java.io.IOException;
import java.util.*;

public abstract class AbstractLoadService {
    /**
     * 用于保存已经创建过的表结构的表名。
     * 对es来说就是index
     * 命名规则：Load类型$targetDbId$tableName   如：“es_mall_user”
     */
    protected Set<String> LOAD_STORAGE = new HashSet<>();

    public Object getTargetDsByDbId(Map<String,Object> allTargetDatasource, String dbId){
        Object ds=allTargetDatasource.get(dbId);
        if(Objects.isNull(ds)){
            throw new ShouldNeverHappenException("target datasource id not found, dbId:"+dbId);
        }
        return ds;
    }

    /**
     * 增删改的总入口，执行这个函数会执行相应的操作
     * @param request
     * @return
     * @throws IOException
     */
    public abstract int operateData(CudRequest request) throws IOException;

    /**
     * 新增一条记录
     * @param request
     * @return
     * @throws IOException
     */
    public abstract int addData(CudRequest request) throws IOException;

    /**
     * 删除一条记录
     * @param request
     * @return
     * @throws IOException
     */
    public abstract int deleteData(CudRequest request) throws IOException;

    /**
     * 更新一条记录
     * @param request
     * @return
     * @throws IOException
     */
    public abstract int updateData(CudRequest request) throws IOException;

    /**
     * 批量新增
     * @param requestList
     * @return
     * @throws IOException
     */
    public abstract int batchAdd(List<CudRequest> requestList) throws IOException;

    /**
     * 统计某个数据库的某个表（index）中的记录数
     * @param dbId
     * @param table
     * @return
     * @throws IOException
     */
    public abstract Long countData(String dbId,String table) throws IOException;

    /**
     * 自动创建表结构
     * @param tableMeta
     * @throws IOException
     */
    public abstract void checkAndCreateStorage(TableMeta tableMeta) throws IOException;
}

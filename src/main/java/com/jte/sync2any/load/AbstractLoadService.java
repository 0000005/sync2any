package com.jte.sync2any.load;

import com.jte.sync2any.conf.SpringContextUtils;
import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.model.config.Conn;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.CollectionUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractLoadService {
    /**
     * 用于保存已经创建过的表结构的表名。
     * 对es来说就是index
     * 命名规则：Load类型$targetDbId$tableName   如：“es_mall_user”
     */
    protected Set<String> LOAD_STORAGE = new HashSet<>();
    private static Map<String,AbstractLoadService> serviceMap = new ConcurrentHashMap<>();

    public Object getTargetDsByDbId(Map<String,Object> allTargetDatasource, String dbId){
        Object ds=allTargetDatasource.get(dbId);
        if(Objects.isNull(ds)){
            throw new ShouldNeverHappenException("target datasource id not found, dbId:"+dbId);
        }
        return ds;
    }

    public static AbstractLoadService getLoadService(String targetType)
    {
        return CollectionUtils.computeIfAbsent(serviceMap,targetType,key->{
            AbstractLoadService loadService;
            if(Conn.DB_TYPE_ES.equals(key))
            {
                loadService = (AbstractLoadService) SpringContextUtils.getContext().getBean("esLoadServiceImpl");
            }
            else if(Conn.DB_TYPE_MYSQL.equals(key))
            {
                loadService = (AbstractLoadService) SpringContextUtils.getContext().getBean("mysqlLoadServiceImpl");
            }
            else
            {
                throw new IllegalArgumentException("同步过程中发现错误的目标类型（targetType）:{"+key+"}，请检查配置文件！");
            }
            return loadService;
        });
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

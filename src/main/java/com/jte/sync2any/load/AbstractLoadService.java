package com.jte.sync2any.load;

import com.jte.sync2any.conf.SpringContextUtils;
import com.jte.sync2any.model.config.Conn;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.CollectionUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            else if(Conn.DB_TYPE_CLICKHOUSE.equals(key))
            {
                loadService = (AbstractLoadService) SpringContextUtils.getContext().getBean("ckLoadServiceImpl");
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
     * 批量新增
     * @param requestList
     * @return
     * @throws IOException
     */
    public abstract int batchAdd(List<CudRequest> requestList) throws IOException;

    /**
     * 载入原始数据时，将保存在缓冲队列的数据全部持久化（ck用）
     * @param
     * @return
     * @throws IOException
     */
    public abstract int flushBatchAdd();

    /**
     * 统计某个数据库的某个表（index）中的记录数
     * 此函数谨慎调用，需看懂它的意思
     * ！！！！注意！！！！
     * 此接口对于每个esIndex在整个应用的生命周期只会查一次，之后的对此函数的调用都会返回第一次查询的值。
     * 这么做的原因是，为了多个分表（rule使用正则表达式）同步到同一个esIndex时，第一次dump数据的完整性。不然只会同步第一个分表的数据。
     * ！！！！注意！！！！
     * 查看es的index是否存在且有数据。
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

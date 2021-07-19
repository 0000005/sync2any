package com.jte.sync2any.load.impl;

import cn.hutool.core.io.file.FileWriter;
import cn.hutool.db.DbUtil;
import cn.hutool.db.GlobalDbConfig;
import cn.hutool.db.handler.NumberHandler;
import cn.hutool.db.sql.SqlExecutor;
import cn.hutool.log.level.Level;
import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.load.AbstractLoadService;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.Field;
import com.jte.sync2any.model.mysql.Row;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.model.mysql.TableRecords;
import com.jte.sync2any.util.DbUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.File;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.jte.sync2any.model.mq.SubscribeDataProto.DMLType.*;

/**
 * @author JerryYin
 * @since 2021-04-06 16:21
 */
@Slf4j
@Service
public class CkLoadServiceImpl extends AbstractLoadService {

    /**
     * INSERT INTO a (x, y, z, pk) VALUES (?, ?, ?, ?),(?, ?, ?, ?),(?, ?, ?, ?)
     */
    private static final String INSERT_SQL_COLUMNS_TEMPLATE = "INSERT INTO %s (%s) VALUES ";

    /**
     * SELECT count(*) FROM a;
     */
    private static final String COUNT_SQL_TEMPLATE = "SELECT COUNT(*) FROM %s ";

    private final String CMT_ENGINE="CollapsingMergeTree";

    @Resource
    Map<String, Object> allTargetDatasource;

    /**
     * 定时任务用于持久化增量数据
     */
    private ScheduledExecutorService scheduledThreadPool = Executors.newSingleThreadScheduledExecutor();

    /**
     * 用来存放待持久化的增量数据
     */
    private static LinkedBlockingQueue<InsertItem> saveQueue = new LinkedBlockingQueue<>();
    /**
     * 用来存放待持久化的存留数据
     * 该集合不存在并发，每次使用只保存同一类型的表数据
     */
    private List<InsertItem> batchAddQueue = new LinkedList<>();
    /**
     * 最新的批量载入的TableMeta
     */
    private TableMeta lastBatchAddTableMeta;

    public CkLoadServiceImpl() {
        GlobalDbConfig.setReturnGeneratedKey(false);
        GlobalDbConfig.setShowSql(true, false, true, Level.DEBUG);
        //初始化持久任务
        log.info("初始化ck持久任务！");
        scheduledThreadPool.scheduleAtFixedRate(new PersistentTask(), 5, 5, TimeUnit.SECONDS);
    }

    /**
     * 此处参数只管往缓冲数组中追加数据（有顺序），并不负责实际的持久化。
     * CollapsingMergeTree系列引擎都默认加sign，其他表引擎只支持新增
     * @param cudRequest
     * @return
     */
    @Override
    public int operateData(CudRequest cudRequest) {
        TableRecords records = cudRequest.getRecords();
        TableMeta tableMeta = cudRequest.getTableMeta();
        boolean isCollapsingMergeTree = tableMeta.getCkTableEngine().contains(CMT_ENGINE);
        if (INSERT == cudRequest.getDmlType()) {
            String valuesSql = buildSingleValuesByRow(records.getNewRows().get(0), tableMeta, isCollapsingMergeTree ? "1" : null);
            saveQueue.add(new InsertItem(tableMeta, valuesSql));
        } else if (UPDATE == cudRequest.getDmlType() && isCollapsingMergeTree) {
            String valuesSqlOld = buildSingleValuesByRow(records.getOldRows().get(0), tableMeta, isCollapsingMergeTree ? "-1" : null);
            String valuesSqlNew = buildSingleValuesByRow(records.getNewRows().get(0), tableMeta, isCollapsingMergeTree ? "1" : null);
            saveQueue.add(new InsertItem(tableMeta, valuesSqlOld));
            saveQueue.add(new InsertItem(tableMeta, valuesSqlNew));
        } else if (DELETE == cudRequest.getDmlType() && isCollapsingMergeTree) {
            String valuesSql = buildSingleValuesByRow(records.getOldRows().get(0), tableMeta, isCollapsingMergeTree ? "-1" : null);
            saveQueue.add(new InsertItem(tableMeta, valuesSql));
        } else {
            log.warn("found unknown operation type({}) when loading. engine:{} table:{}", cudRequest.getDmlType(), tableMeta.getCkTableEngine(), tableMeta.getTargetTableName());
        }
        return 1;
    }

    @Override
    public int batchAdd(List<CudRequest> requestList) {
        return batchAdd(requestList,false);
    }

    /**
     * TODO 暂不支持dynamic_tablename_assigner分发到多个表
     * 批量新增
     * @param requestList
     * @param isForce 是否强制入库
     * @return
     */
    public int batchAdd(List<CudRequest> requestList, boolean isForce) {
        log.debug("ck batchAdd requestList size:{} isForce:{}",requestList.size(),isForce);
        //总待入库的请求为空而且当前待入库的请求为空，则直接返回结束
        if(batchAddQueue.isEmpty() && requestList.isEmpty()){
            return 0;
        }
        //非强制入库且当前待入库的请求为空，则直接返回结束
        if(requestList.isEmpty() && !isForce){
            return 0;
        }
        //强制入库，但总待入库和当前待入库的请求都为空，则直接返回结束
        if(batchAddQueue.isEmpty() && requestList.isEmpty() && isForce){
            return 0;
        }
        for(CudRequest request : requestList){
            TableMeta tableMeta = request.getTableMeta();
            boolean isCollapsingMergeTree = tableMeta.getCkTableEngine().contains(CMT_ENGINE);
            String valuesSql = buildSingleValuesByReq(request, tableMeta, isCollapsingMergeTree ? "1" : null);
            batchAddQueue.add(new InsertItem(tableMeta, valuesSql));
        }

        if(!requestList.isEmpty()){
            lastBatchAddTableMeta=requestList.get(0).getTableMeta();
        }

        //每超过10000行记录入库一次
        if(batchAddQueue.size()<10000 && !isForce){
            return 0;
        }
        //////////////////////开始真正入库/////////////////////////
        List<String> valueList = new ArrayList<>();
        for(int i =0;i<batchAddQueue.size();i++){
            valueList.add(batchAddQueue.get(i).getValuesSql());
        }

        TableMeta tableMeta = lastBatchAddTableMeta;
        log.debug("batch save size:{} table:{}",batchAddQueue.size(),tableMeta.getTargetTableName());
        String sql = generateCompleteInsertSql(tableMeta,valueList);

        DataSource ds = (DataSource) allTargetDatasource.get(tableMeta.getTargetDbId());
        saveToCk(sql,ds);
        //清空缓冲数组
        batchAddQueue.clear();

        return batchAddQueue.size();
    }

    @Override
    public int flushBatchAdd() {
        return batchAdd(new ArrayList<>(),true);
    }

    @Override
    public Long countData(String dbId, String table) {
        String key = dbId+"-"+table;
        Long count= cacheInitCount.get(key);
        if(Objects.nonNull(count))
        {
            return count;
        }
        DataSource ds = (DataSource) DbUtils.getTargetDsByDbId(allTargetDatasource, dbId);
        Connection connection=null;
        try {
            connection = ds.getConnection();
            String countSql = String.format(COUNT_SQL_TEMPLATE, table);
            Number n = SqlExecutor.query(connection,countSql, new NumberHandler());
            count=n.longValue();
            cacheInitCount.put(key,count);
            return count;
        }catch (Exception e){
            log.error("count error:",e);
            throw new ShouldNeverHappenException("count error");
        }finally {
            DbUtil.close(connection);
        }
    }

    @Override
    public void checkAndCreateStorage(TableMeta tableMeta) {

    }


    /**
     * 构建insert sql语句的列部分
     *
     * @param tableMeta
     * @return
     */
    public String buildInsertColumnsSql(TableMeta tableMeta) {
        String insertColumns = tableMeta.getAllColumnList().stream()
                .filter(c -> c.isInclude())
                .map(c -> c.getTargetColumnName())
                .collect(Collectors.joining(", "));
        //CollapsingMergeTree系列需默认追加_sign列。
        if (tableMeta.getCkTableEngine().contains(CMT_ENGINE)) {
            insertColumns = insertColumns + ",_sign";
        }
        return String.format(INSERT_SQL_COLUMNS_TEMPLATE, tableMeta.getTargetTableName(), insertColumns);
    }

    private String buildSingleValuesByReq(CudRequest request, TableMeta tableMeta, String sign) {
        Map<String, Object> paramMap = request.getParameters();

        String insertValues = tableMeta.getAllColumnList().stream()
                .filter(c -> c.isInclude())
                .map(c -> {
                    Object value = paramMap.get(c.getTargetColumnName());
                    int dataType = c.getDataType();
                    if (Objects.isNull(value)) {
                        return columnIsNumber(dataType)?"0":"''";
                    } else if (columnIsNumber(dataType)) {
                        return value.toString();
                    } else {
                        return "'" + value.toString() + "'";
                    }
                }).collect(Collectors.joining(", "));
        if (StringUtils.isNotBlank(sign)) {
            insertValues = insertValues + "," + sign;
        }
        return insertValues;
    }

    private String buildSingleValuesByRow(Row row, TableMeta tableMeta, String sign) {
        String insertValues = tableMeta.getAllColumnList().stream()
                .filter(c -> c.isInclude())
                .map(c -> {
                    Field field = row.getFields().stream().filter(e -> e.getName().equalsIgnoreCase(c.getColumnName())).findFirst().orElse(null);
                    int dataType = c.getDataType();
                    if (Objects.isNull(field.getValue())) {
                        return columnIsNumber(dataType)?"0":"''";
                    } else if (columnIsNumber(dataType)) {
                        return field.getValue().toString();
                    } else {
                        return "'" + field.getValue().toString() + "'";
                    }
                }).collect(Collectors.joining(", "));
        if (StringUtils.isNotBlank(sign)) {
            insertValues = insertValues + "," + sign;
        }
        return insertValues;
    }


    public boolean columnIsNumber(int dataType) {
        if (dataType == Types.BIGINT || dataType == Types.INTEGER || dataType == Types.TINYINT || dataType == Types.SMALLINT || dataType == Types.FLOAT || dataType == Types.DOUBLE || dataType == Types.DECIMAL  || dataType == Types.NUMERIC) {
            return true;
        }
        return false;
    }

    @Data
    class InsertItem {
        private TableMeta tableMeta;
        private String valuesSql;

        public InsertItem(TableMeta tableMeta, String valuesSql) {
            this.tableMeta = tableMeta;
            this.valuesSql = valuesSql;
        }
    }

    class PersistentTask implements Runnable {

        @Override
        public void run() {
            try{
                Map<TableMeta, List<String>> dataMap = new HashMap<>();
                int currentQueueSize = new Integer(saveQueue.size());
                //每次最多1万
                currentQueueSize = currentQueueSize > 10000 ? 10000 : currentQueueSize;
                log.debug("ck currentQueueSize:{}", currentQueueSize);
                for (int i = 0; i < currentQueueSize; i++) {
                    InsertItem insertItem = saveQueue.poll();
                    if (Objects.isNull(insertItem)) {
                        break;
                    }
                    List<String> valueList = dataMap.get(insertItem.getTableMeta());
                    if (Objects.isNull(valueList)) {
                        valueList = new ArrayList<>();
                        valueList.add(insertItem.valuesSql);
                        dataMap.put(insertItem.getTableMeta(), valueList);
                    } else {
                        valueList.add(insertItem.valuesSql);
                    }
                }
                Set<TableMeta> tableMetaSet = dataMap.keySet();
                log.debug("ck tableMetaSize:{}", tableMetaSet.size());
                //分组生成完整的insert语句
                for (TableMeta tableMeta : tableMetaSet) {
                    String sql = generateCompleteInsertSql(tableMeta,dataMap.get(tableMeta));
                    log.debug("sql:{}", sql);
                    //入库
                    DataSource ds = (DataSource) allTargetDatasource.get(tableMeta.getTargetDbId());
                    saveToCk(sql.toString(), ds);
                }
            }catch (Exception e){
                log.error("定时持久化异常：",e);
            }

        }
    }


    /**
     * 生成一条完整的insertSql
     * @param tableMeta
     * @param valuesList
     * @return
     */
    private String generateCompleteInsertSql(TableMeta tableMeta ,List<String> valuesList){
        StringBuilder sql = new StringBuilder();
        //列部分
        sql.append(buildInsertColumnsSql(tableMeta));
        log.debug("ck valuesListSize:{}", valuesList.size());
        //值部分
        for (int i = 0; i < valuesList.size(); i++) {
            sql.append("(");
            sql.append(valuesList.get(i));
            sql.append(")");
            if (i != valuesList.size() - 1) {
                sql.append(",");
            }
        }
        return sql.toString();
    }

    /**
     * 持久化
     *
     * @param sql
     * @param ds
     * @return
     */
    public int saveToCk(String sql, DataSource ds) {
        Connection conn = null;
        try {
            conn = ds.getConnection();
            return SqlExecutor.execute(conn, sql);
        } catch (SQLException e) {
            log.error("execute sql error:", e);
            String filePath=System.getProperty("java.io.tmpdir")+File.separator+new SecureRandom().nextInt(9999999)+".error.sql";
            FileWriter writer = new FileWriter(filePath);
            writer.write(sql);
            throw new ShouldNeverHappenException("execute sql error,error sql:"+filePath);
        } finally {
            DbUtil.close(conn);
        }
    }

    public static boolean isSaveQueueEmpty(){
        return saveQueue.isEmpty();
    }
}

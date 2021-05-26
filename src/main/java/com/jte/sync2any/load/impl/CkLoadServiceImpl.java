package com.jte.sync2any.load.impl;

import cn.hutool.db.DbUtil;
import cn.hutool.db.sql.SqlExecutor;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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

    @Resource
    Map<String, Object> allTargetDatasource;

    private ScheduledExecutorService scheduledThreadPool = new ScheduledThreadPoolExecutor(1);

    /**
     * 用来存放待持久化的数据
     */
    private LinkedBlockingQueue<InsertItem> saveQueue = new LinkedBlockingQueue<>(3);

    public CkLoadServiceImpl() {
        //初始化持久任务
        scheduledThreadPool.scheduleAtFixedRate(new PersistentTask(),5,5,TimeUnit.SECONDS);
    }

    /**
     * 此处参数只管往缓冲数组中追加数据（有顺序），并不负责实际的持久化。
     *
     * @param cudRequest
     * @return
     */
    @Override
    public int operateData(CudRequest cudRequest) {
        TableMeta tableMeta = cudRequest.getTableMeta();
        boolean isVersionedCollapsingMergeTree = tableMeta.getCkTableEngine().contains("VersionedCollapsingMergeTree");
        TableRecords records = cudRequest.getRecords();
        if (INSERT == cudRequest.getDmlType()) {
            String valuesSql = buildValuesByRow(records.getNewRows().get(0), tableMeta, isVersionedCollapsingMergeTree ? "1" : null);
            saveQueue.add(new InsertItem(tableMeta, valuesSql));
        } else if (UPDATE == cudRequest.getDmlType()) {
            String valuesSqlOld = buildValuesByRow(records.getOldRows().get(0), tableMeta, isVersionedCollapsingMergeTree ? "-1" : null);
            String valuesSqlNew = buildValuesByRow(records.getNewRows().get(0), tableMeta, isVersionedCollapsingMergeTree ? "1" : null);
            saveQueue.add(new InsertItem(tableMeta, valuesSqlOld));
            saveQueue.add(new InsertItem(tableMeta, valuesSqlNew));
        } else if (DELETE == cudRequest.getDmlType()) {
            String valuesSql = buildValuesByRow(records.getOldRows().get(0), tableMeta, isVersionedCollapsingMergeTree ? "-1" : null);
            saveQueue.add(new InsertItem(tableMeta, valuesSql));
        } else {
            throw new ShouldNeverHappenException("unknown operation type:" + cudRequest.getDmlType());
        }
        return 1;
    }

    @Override
    public int batchAdd(List<CudRequest> requestList) {
        return 0;
    }

    @Override
    public Long countData(String dbId, String table) {
        JdbcTemplate jdbcTemplate = (JdbcTemplate) DbUtils.getTargetDsByDbId(allTargetDatasource, dbId);
        String countSql = String.format(COUNT_SQL_TEMPLATE, table);
        int count = jdbcTemplate.queryForObject(countSql, Integer.class);
        return Long.valueOf(count);
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
        //VersionedCollapsingMergeTree系列需默认追加_sign列。
        if (tableMeta.getCkTableEngine().contains("VersionedCollapsingMergeTree")) {
            insertColumns = insertColumns + ",_sign";
        }
        return String.format(INSERT_SQL_COLUMNS_TEMPLATE, tableMeta.getTargetTableName(), insertColumns);
    }


    private String buildValuesByRow(Row row, TableMeta tableMeta, String sign) {
        String insertValues = tableMeta.getAllColumnList().stream()
                .filter(c -> c.isInclude())
                .map(c -> {
                    Field field = row.getFields().stream().filter(e -> e.getName().equalsIgnoreCase(c.getColumnName())).findFirst().orElse(null);
                    int dataType = c.getDataType();
                    if (columnIsNumber(dataType)) {
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
        if (dataType == Types.BIGINT || dataType == Types.INTEGER || dataType == Types.TINYINT || dataType == Types.SMALLINT || dataType == Types.FLOAT || dataType == Types.DOUBLE || dataType == Types.NUMERIC) {
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
            Map<TableMeta, List<String>> dataMap = new HashMap<>();
            int currentQueueSize = new Integer(saveQueue.size());
            //每次最多1万
            currentQueueSize = currentQueueSize > 10000 ? 10000 : currentQueueSize;
            for (int i = 0; i < currentQueueSize; i++) {
                InsertItem insertItem = saveQueue.poll();
                if (Objects.isNull(insertItem)){
                    break;
                }
                List<String> valueList = dataMap.get(insertItem);
                if (Objects.isNull(valueList)) {
                    valueList = new ArrayList<>();
                    valueList.add(insertItem.valuesSql);
                    dataMap.put(insertItem.getTableMeta(), valueList);
                } else {
                    valueList.add(insertItem.valuesSql);
                }
            }
            //分组生成完整的insert语句
            for(TableMeta tableMeta : dataMap.keySet()){
                StringBuilder sql = new StringBuilder();
                //列部分
                sql.append(buildInsertColumnsSql(tableMeta));
                List<String> valuesList = dataMap.get(sql);
                //值部分
                for(int i = 0; i<valuesList.size() ; i++){
                    sql.append("(");
                    sql.append(valuesList.get(i));
                    sql.append(")");
                    if(i!=valuesList.size() -1){
                        sql.append(",");
                    }
                }
                log.debug("sql:{}",sql);
                //入库
                DataSource ds = (DataSource) allTargetDatasource.get(tableMeta.getTargetDbId());
                saveToCk(sql.toString(),ds);
            }
        }
    }

    /**
     * 持久化
     * @param sql
     * @param ds
     * @return
     */
    public int saveToCk(String sql,DataSource ds){
        Connection conn = null;
        try {
            conn = ds.getConnection();
            return SqlExecutor.execute(conn, sql);
        } catch (SQLException e) {
            log.error("execute sql error:",e);
            throw new ShouldNeverHappenException("execute sql error");
        } finally {
            DbUtil.close(conn);
        }
    }
}

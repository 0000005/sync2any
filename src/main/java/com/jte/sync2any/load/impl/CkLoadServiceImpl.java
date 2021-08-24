package com.jte.sync2any.load.impl;

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
import java.sql.Connection;
import java.sql.PreparedStatement;
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
    private static final String INSERT_SQL_COLUMNS_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";

    /**
     * SELECT count(*) FROM a;
     */
    private static final String COUNT_SQL_TEMPLATE = "SELECT COUNT(*) FROM %s ";

    private final String CMT_ENGINE = "CollapsingMergeTree";

    /**
     * 每次执行定时任务最大的处理数量
     */
    private final int maxHandleNumPerTask = 20000;

    /**
     * 每次批量新增任务最大的处理数量
     */
    private final int maxHandleNumPerBatchAdd = 20000;

    /**
     * 每次传到ck的sql最大空间大小（byte）,该值如果过大，可能会导致内存溢出（在mysqldump时，每条sql比较大的情况下）。
     * String 空间占用：https://www.cnblogs.com/binghe001/p/13860617.html
     * 40 + 2 * n；默认500M
     * 因为不方便得到sql的长度，因此暂不启动该条件
     */
    //private final int maxSentSizePerAdd = 40 + (2 * 250000000);

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
    private LinkedBlockingQueue<InsertItem> batchAddQueue = new LinkedBlockingQueue<>();
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
     *
     * @param cudRequest
     * @return
     */
    @Override
    public int operateData(CudRequest cudRequest) {
        TableRecords records = cudRequest.getRecords();
        TableMeta tableMeta = cudRequest.getTableMeta();
        boolean isCollapsingMergeTree = tableMeta.getCkTableEngine().contains(CMT_ENGINE);
        if (INSERT == cudRequest.getDmlType()) {
            List<Object> valueList = buildSingleValuesByRow(records.getNewRows().get(0), tableMeta, isCollapsingMergeTree ? "1" : null);
            saveQueue.add(new InsertItem(tableMeta, valueList));
        } else if (UPDATE == cudRequest.getDmlType() && isCollapsingMergeTree) {
            List<Object> oldValueList = buildSingleValuesByRow(records.getOldRows().get(0), tableMeta, isCollapsingMergeTree ? "-1" : null);
            List<Object> newValueList = buildSingleValuesByRow(records.getNewRows().get(0), tableMeta, isCollapsingMergeTree ? "1" : null);
            saveQueue.add(new InsertItem(tableMeta, oldValueList));
            saveQueue.add(new InsertItem(tableMeta, newValueList));
        } else if (DELETE == cudRequest.getDmlType() && isCollapsingMergeTree) {
            List<Object> valueList = buildSingleValuesByRow(records.getOldRows().get(0), tableMeta, isCollapsingMergeTree ? "-1" : null);
            saveQueue.add(new InsertItem(tableMeta, valueList));
        } else {
            log.warn("found unknown operation type({}) when loading. engine:{} table:{}", cudRequest.getDmlType(), tableMeta.getCkTableEngine(), tableMeta.getTargetTableName());
        }
        return 1;
    }

    @Override
    public int batchAdd(List<CudRequest> requestList) {
        return batchAdd(requestList, false);
    }

    /**
     * TODO 暂不支持dynamic_tablename_assigner分发到多个表
     * 批量新增
     *
     * @param requestList
     * @param isForce     是否强制入库
     * @return
     */
    public int batchAdd(List<CudRequest> requestList, boolean isForce) {
        log.info("ck batchAdd requestList size:{} isForce:{}", requestList.size(), isForce);
        //总待入库的请求为空而且当前待入库的请求为空，则直接返回结束
        if (batchAddQueue.isEmpty() && requestList.isEmpty()) {
            return 0;
        }
        //非强制入库且当前待入库的请求为空，则直接返回结束
        if (requestList.isEmpty() && !isForce) {
            return 0;
        }
        //强制入库，但总待入库和当前待入库的请求都为空，则直接返回结束
        if (batchAddQueue.isEmpty() && requestList.isEmpty() && isForce) {
            return 0;
        }
        requestList.stream().parallel().forEach((request -> {
            TableMeta tableMeta = request.getTableMeta();
            boolean isCollapsingMergeTree = tableMeta.getCkTableEngine().contains(CMT_ENGINE);
            List<Object> valueList = buildSingleValuesByReq(request, tableMeta, isCollapsingMergeTree ? "1" : null);
            batchAddQueue.add(new InsertItem(tableMeta, valueList));
        }));

        if (!requestList.isEmpty()) {
            lastBatchAddTableMeta = requestList.get(0).getTableMeta();
        }

        //每超过N行记录入库一次
        if (batchAddQueue.size() < maxHandleNumPerBatchAdd && !isForce) {
            return 0;
        }
        //////////////////////开始真正入库/////////////////////////
        List<Object> valueList = new ArrayList<>();
        int batchAddQueueSize = batchAddQueue.size();
        for (int i = 0; i < batchAddQueueSize; i++) {
            valueList.addAll(batchAddQueue.poll().getValueList());
        }

        TableMeta tableMeta = lastBatchAddTableMeta;
        String sql = generateCompleteInsertSql(tableMeta, valueList);

        DataSource ds = (DataSource) allTargetDatasource.get(tableMeta.getTargetDbId());
        saveToCk(sql, ds, valueList, batchAddQueueSize);
        log.info("batch save size:{} table:{}", batchAddQueueSize, tableMeta.getTargetTableName());
        //清空缓冲数组
        batchAddQueue.clear();

        return batchAddQueue.size();
    }

    @Override
    public int flushBatchAdd() {
        return batchAdd(new ArrayList<>(), true);
    }

    @Override
    public Long countData(String dbId, String table) {
        String key = dbId + "-" + table;
        Long count = cacheInitCount.get(key);
        if (Objects.nonNull(count)) {
            return count;
        }
        DataSource ds = (DataSource) DbUtils.getTargetDsByDbId(allTargetDatasource, dbId);
        Connection connection = null;
        try {
            connection = ds.getConnection();
            String countSql = String.format(COUNT_SQL_TEMPLATE, table);
            Number n = SqlExecutor.query(connection, countSql, new NumberHandler());
            count = n.longValue();
            cacheInitCount.put(key, count);
            return count;
        } catch (Exception e) {
            log.error("count error:", e);
            throw new ShouldNeverHappenException("count error");
        } finally {
            DbUtil.close(connection);
        }
    }

    @Override
    public void checkAndCreateStorage(TableMeta tableMeta) {

    }


    private List<Object> buildSingleValuesByReq(CudRequest request, TableMeta tableMeta, String sign) {
        Map<String, Object> paramMap = request.getParameters();
        List<Object> valuesList = new ArrayList<>();
        tableMeta.getAllColumnList().stream()
                .filter(c -> c.isInclude())
                .forEach(c -> {
                    Object value = paramMap.get(c.getTargetColumnName());
                    int dataType = c.getDataType();
                    if (Objects.isNull(value)) {
                        value = (columnIsNumber(dataType) || columnIsDecimal(dataType)) ? 0 : null;
                    } else if (columnIsNumber(dataType)) {
                        value = Long.valueOf(value.toString());
                    } else if (columnIsDecimal(dataType)) {
                        value = Double.valueOf(value.toString());
                    } else {
                        value = value.toString();
                    }
                    valuesList.add(value);
                });
        if (StringUtils.isNotBlank(sign)) {
            valuesList.add(sign);
        }
        return valuesList;
    }

    private List<Object> buildSingleValuesByRow(Row row, TableMeta tableMeta, String sign) {
        List<Object> valuesList = new ArrayList<>();
        tableMeta.getAllColumnList().stream()
                .filter(c -> c.isInclude())
                .forEach(c -> {
                    Field field = row.getFields().stream().filter(e -> e.getName().equalsIgnoreCase(c.getColumnName())).findFirst().orElse(null);
                    int dataType = c.getDataType();
                    Object value = null;
                    if (Objects.isNull(field.getValue())) {
                        value = (columnIsNumber(dataType) || columnIsDecimal(dataType)) ? 0 : null;
                    } else if (columnIsNumber(dataType)) {
                        value = Long.valueOf(field.getValue().toString());
                    } else if (columnIsDecimal(dataType)) {
                        value = Double.valueOf(field.getValue().toString());
                    } else {
                        value = field.getValue().toString();
                    }
                    valuesList.add(value);
                });
        if (StringUtils.isNotBlank(sign)) {
            valuesList.add(sign);
        }
        return valuesList;
    }


    /**
     * 是否为整数
     *
     * @param dataType
     * @return
     */
    public boolean columnIsNumber(int dataType) {
        if (dataType == Types.BIGINT || dataType == Types.INTEGER || dataType == Types.TINYINT || dataType == Types.SMALLINT || dataType == Types.NUMERIC) {
            return true;
        }
        return false;
    }

    /**
     * 是否为小数
     *
     * @param dataType
     * @return
     */
    public boolean columnIsDecimal(int dataType) {
        if (dataType == Types.FLOAT || dataType == Types.DOUBLE || dataType == Types.DECIMAL) {
            return true;
        }
        return false;
    }

    @Data
    class InsertItem {
        private TableMeta tableMeta;
        private List<Object> valueList;

        public InsertItem(TableMeta tableMeta, List<Object> valueList) {
            this.tableMeta = tableMeta;
            this.valueList = valueList;
        }
    }

    class PersistentTask implements Runnable {

        @Override
        public void run() {
            try {
                //如果队列里面一直有元素，则会一直跑。
                while(saveQueue.size() > 0){
                    /**
                     * 获取插入值，按表分组
                     */
                    Map<TableMeta, List<Object>> dataMap = new HashMap<>();
                    /**
                     * 获取insert数量，按表分组
                     */
                    Map<TableMeta, Integer> countMap = new HashMap<>();
                    int currentQueueSize = new Integer(saveQueue.size());
                    //每次最多1万
                    currentQueueSize = currentQueueSize > maxHandleNumPerTask ? maxHandleNumPerTask : currentQueueSize;
                    if (currentQueueSize > 0) {
                        log.info("ck currentQueueSize:{}", currentQueueSize);
                    }
                    for (int i = 0; i < currentQueueSize; i++) {
                        InsertItem insertItem = saveQueue.poll();
                        if (Objects.isNull(insertItem)) {
                            break;
                        }
                        List<Object> valueList = dataMap.get(insertItem.getTableMeta());
                        Integer count = countMap.get(insertItem.getTableMeta());
                        if (Objects.isNull(valueList)) {
                            valueList = new ArrayList<>();
                            valueList.addAll(insertItem.getValueList());
                            dataMap.put(insertItem.getTableMeta(), valueList);
                            countMap.put(insertItem.getTableMeta(), 1);
                        } else {
                            valueList.addAll(insertItem.getValueList());
                            //sql数量加1
                            countMap.put(insertItem.getTableMeta(), ++count);
                        }
                    }
                    Set<TableMeta> tableMetaSet = dataMap.keySet();
                    if (tableMetaSet.size() > 0) {
                        log.info("ck tableMetaSize:{}", tableMetaSet.size());
                    }
                    //分组生成完整的insert语句
                    for (TableMeta tableMeta : tableMetaSet) {

                        String sql = generateCompleteInsertSql(tableMeta, dataMap.get(tableMeta));
                        //入库
                        DataSource ds = (DataSource) allTargetDatasource.get(tableMeta.getTargetDbId());
                        saveToCk(sql, ds, dataMap.get(tableMeta), countMap.get(tableMeta));
                    }
                }
                log.info("execute PersistentTask over.");
            } catch (Exception e) {
                log.error("定时持久化异常：", e);
            }

        }
    }


    /**
     * 生成一条完整的insertSql
     *
     * @param tableMeta
     * @param valuesList
     * @return
     */
    private String generateCompleteInsertSql(TableMeta tableMeta, List<Object> valuesList) {
        String insertColumnNames = tableMeta.getAllColumnList().stream()
                .filter(c -> c.isInclude())
                .map(c -> c.getTargetColumnName())
                .collect(Collectors.joining(", "));
        String placeHolders = tableMeta.getAllColumnList().stream()
                .filter(c -> c.isInclude())
                .map(c -> "?")
                .collect(Collectors.joining(", "));
        //CollapsingMergeTree系列需默认追加_sign列。
        if (tableMeta.getCkTableEngine().contains(CMT_ENGINE)) {
            insertColumnNames = insertColumnNames + ",_sign";
            placeHolders = placeHolders + ",?";
        }
        return String.format(INSERT_SQL_COLUMNS_TEMPLATE, tableMeta.getTargetTableName(), insertColumnNames, placeHolders);
    }


    /**
     * 持久化
     *
     * @param sql
     * @param ds
     * @return
     */
    public long saveToCk(String sql, DataSource ds, List<Object> valueList, int sqlSize) {
        Connection connection = null;
        PreparedStatement ps = null;
        long columnNum = valueList.size() / sqlSize;
        log.debug("sql:{}", sql);
        log.debug("valueList.size():{} columnNum:{} sqlSize:{}", valueList.size(), columnNum, sqlSize);
        try {
            connection = ds.getConnection();
            connection.setAutoCommit(true);
            ps = connection.prepareStatement(sql);
            int valueIndex = 0;
            for (int i = 0; i < sqlSize; i++) {
                for (int c = 1; c <= columnNum; c++) {
                    ps.setObject(c, valueList.get(valueIndex));
                    valueIndex++;
                }
                ps.addBatch();
            }
            int[] affectArray = ps.executeBatch();
            return affectArray.length;
        } catch (Exception e) {
            log.error("saveToCk error", e);
            throw new ShouldNeverHappenException(e);
        } finally {
            DbUtil.close(ps, connection);
        }
    }

    public static boolean isSaveQueueEmpty() {
        return saveQueue.isEmpty();
    }
}

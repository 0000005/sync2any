package com.jte.sync2any.load.impl;

import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.load.AbstractLoadService;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.ColumnMeta;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.ColumnUtils;
import com.jte.sync2any.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
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
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES %s";

    /**
     * SELECT count(*) FROM a;
     */
    private static final String COUNT_SQL_TEMPLATE = "SELECT COUNT(*) FROM %s ";

    @Resource
    Map<String, Object> allTargetDatasource;

    /**
     * 此处参数只管往缓冲数组中追加数据（有顺序），并不负责实际的持久化。
     *
     * @param request
     * @return
     */
    @Override
    public int operateData(CudRequest request) {
        log.info("newRowSize:{}  oldRowSize:{}" , request.getRecords().getNewRows().size(), request.getRecords().getOldRows().size());

        if (INSERT == request.getDmlType()) {
            addData(request);
        } else if (UPDATE == request.getDmlType()) {
            updateData(request);
        } else if (DELETE == request.getDmlType()) {
            deleteData(request);
        } else {
            throw new ShouldNeverHappenException("unknown operation type:" + request.getDmlType());
        }
        return 1;
    }

    /**
     * 追加的一条sign为1，version+1的记录
     *
     * @param request
     * @return
     */
    private void addData(CudRequest request) {

    }

    /**
     * 追加一条记录sign为-1，version
     *
     * @param request
     * @return
     */
    private void deleteData(CudRequest request) {

    }

    /**
     * 追加两条记录
     * 1、sign为-1，version
     * 2、sign为1，version+1
     */
    private void updateData(CudRequest request) {
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
     * 为insert语句填充参数
     *
     * @param cudRequest
     * @return
     */
    public Object[] fillUpdateParam(CudRequest cudRequest) {
        TableMeta tableMeta = cudRequest.getTableMeta();
        Object[] params = new Object[tableMeta.getAllColumnList().size()];
        int index = 0;
        List<String> pkNameList = tableMeta.getPrimaryKeyOnlyName();
        List<ColumnMeta> nonPkColumnMateList = tableMeta.getAllColumnList().stream()
                .filter(e -> !pkNameList.contains(e.getColumnName()))
                .collect(Collectors.toList());
        for (ColumnMeta columnMeta : nonPkColumnMateList) {
            params[index] = cudRequest.getParameters().get(columnMeta.getColumnName());
            index++;
        }
        for (String pkColumnName : tableMeta.getPrimaryKeyOnlyName()) {
            params[index] = cudRequest.getPkValueMap().get(pkColumnName).getValue();
            index++;
        }
        return params;
    }


    /**
     * 构建insert sql语句
     *
     * @param cudRequest
     * @return
     */
    public String buildInsertSqlByPks(CudRequest cudRequest) {
        TableMeta tableMeta = cudRequest.getTableMeta();
        String insertColumns = tableMeta.getAllColumnList().stream()
                .map(field -> ColumnUtils.addEscape(field.getColumnName()))
                .collect(Collectors.joining(", "));
        String insertValues = tableMeta.getAllColumnList().stream().map(field -> "?")
                .collect(Collectors.joining(", "));

        return String.format(INSERT_SQL_TEMPLATE, cudRequest.getTable(), insertColumns, insertValues);
    }

    /**
     * 为insert语句填充参数
     *
     * @param cudRequest
     * @return 返回一个数组，数组中的元素对应sql中的placeholder
     */
    public Object[] fillInsertParam(CudRequest cudRequest) {
        TableMeta tableMeta = cudRequest.getTableMeta();
        Object[] params = new Object[tableMeta.getAllColumnList().size()];
        int i = 0;
        for (ColumnMeta columnMeta : tableMeta.getAllColumnList()) {
            params[i] = cudRequest.getParameters().get(columnMeta.getColumnName());
            i++;
        }
        return params;
    }
}

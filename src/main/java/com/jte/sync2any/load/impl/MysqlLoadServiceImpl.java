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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.jte.sync2any.model.mq.SubscribeDataProto.DMLType.*;

/**
 * @author JerryYin
 * @since 2021-04-06 16:21
 */
@Slf4j
@Service
public class MysqlLoadServiceImpl extends AbstractLoadService {

    /**
     * INSERT INTO a (x, y, z, pk) VALUES (?, ?, ?, ?)
     */
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";

    /**
     * UPDATE a SET x = ?, y = ?, z = ? WHERE pk1 in (?) pk2 in (?)
     */
    private static final String UPDATE_SQL_TEMPLATE = "UPDATE %s SET %s WHERE %s ";

    /**
     * DELETE FROM a WHERE pk1 in ?;
     */
    private static final String DELETE_SQL_TEMPLATE = "DELETE FROM %s WHERE %s ";

    /**
     * SELECT count(*) FROM a;
     */
    private static final String COUNT_SQL_TEMPLATE = "SELECT COUNT(*) FROM %s ";

    @Resource
    Map<String, Object> allTargetDatasource;

    @Override
    public int operateData(CudRequest request) {
        JdbcTemplate jdbcTemplate = (JdbcTemplate) DbUtils.getTargetDsByDbId(allTargetDatasource, request.getTableMeta().getTargetDbId());
        if (INSERT == request.getDmlType()) {
            return addData(request, jdbcTemplate);
        } else if (UPDATE == request.getDmlType()) {
            return updateData(request, jdbcTemplate);
        } else if (DELETE == request.getDmlType()) {
            return deleteData(request, jdbcTemplate);
        } else {
            throw new ShouldNeverHappenException("unknown operation type:" + request.getDmlType());
        }
    }

    private int addData(CudRequest request, JdbcTemplate jdbcTemplate) {
        String sql = buildInsertSqlByPks(request);
        Object[] params = fillInsertParam(request);
        log.debug("sql:{},params:{}", sql,params);
        return jdbcTemplate.update(sql,params);
    }

    private int deleteData(CudRequest request, JdbcTemplate jdbcTemplate) {
        TableMeta tableMeta = request.getTableMeta();
        String whereSql = buildWhereConditionSqlByPks(tableMeta);
        String deleteSql = String.format(DELETE_SQL_TEMPLATE, request.getTable(), whereSql);
        Object[] params = fillDeleteParam(request);
        log.debug("sql:{},params:{}", deleteSql,params);
        return jdbcTemplate.update(deleteSql,params);
    }

    private int updateData(CudRequest request, JdbcTemplate jdbcTemplate) {
        String sql = buildUpdateSqlByPks(request);
        Object[] params = fillUpdateParam(request);
        log.debug("sql:{},params:{}", sql,params);
        return jdbcTemplate.update(sql,params);
    }

    @Override
    public int batchAdd(List<CudRequest> requestList) {
        int effectNums = 0;
        Map<TableMeta, List<CudRequest>> requestMap = requestList.stream().parallel().collect(Collectors.groupingBy(CudRequest::getTableMeta));
        Set<TableMeta> tableMeteSet = requestMap.keySet();
        Iterator<TableMeta> it = tableMeteSet.iterator();
        while (it.hasNext()) {
            TableMeta tableMeta = it.next();
            JdbcTemplate jdbcTemplate = (JdbcTemplate) DbUtils.getTargetDsByDbId(allTargetDatasource, tableMeta.getTargetDbId());
            List<CudRequest> groupByList = requestMap.get(tableMeta);
            for (CudRequest e : groupByList) {
                effectNums += addData(e, jdbcTemplate);
            }
        }
        return effectNums;
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
     * 构建update sql，更新值不包括主键；where以主键为条件
     *
     * @param cudRequest
     * @return
     */
    public String buildUpdateSqlByPks(CudRequest cudRequest) {
        TableMeta tableMeta = cudRequest.getTableMeta();
        List<String> pkNameList = tableMeta.getPrimaryKeyOnlyName();
        String updateColumns = tableMeta.getAllColumnList().stream()
                .filter(e -> !pkNameList.contains(e.getColumnName()))
                .map(c -> ColumnUtils.addEscape(c.getColumnName()) + " = ?")
                .collect(Collectors.joining(", "));
        String whereSql = buildWhereConditionSqlByPks(tableMeta);
        return String.format(UPDATE_SQL_TEMPLATE, cudRequest.getTable(), updateColumns, whereSql);
    }


    /**
     * 构建where的部分的sql语句，以主键为参数。
     * 在delete和update时可以用到。
     *
     * @param tableMeta
     * @return
     */
    public String buildWhereConditionSqlByPks(TableMeta tableMeta) {
        StringBuilder whereStr = new StringBuilder();
        List<String> pkNameList = tableMeta.getPrimaryKeyOnlyName();
        int i = 0;
        //we must consider the situation of composite primary key
        for (String pkName : pkNameList) {
            if (i > 0) {
                whereStr.append(" and ");
            }
            whereStr.append(ColumnUtils.addEscape(pkName));
            whereStr.append(" = ? ");
            i++;
        }
        return whereStr.toString();
    }

    /**
     * 为delete语句填充参数
     *
     * @param cudRequest
     * @return 返回一个数组，数组中的元素对应sql中的placeholder
     */
    public Object[] fillDeleteParam(CudRequest cudRequest) {
        TableMeta tableMeta = cudRequest.getTableMeta();
        Object[] params = new Object[tableMeta.getPrimaryKeyOnlyName().size()];
        int i = 0;
        for (String pkName : tableMeta.getPrimaryKeyOnlyName()) {
            params[i] = cudRequest.getPkValueMap().get(pkName).getValue();
            i++;
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

package com.jte.sync2any.load;

import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.ColumnMeta;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.ColumnUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author JerryYin
 * @since 2021-04-06 16:21
 */
public class MysqlLoadServiceImpl extends AbstractLoadService {

    /**
     * INSERT INTO a (x, y, z, pk) VALUES (?, ?, ?, ?)
     */
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";

    @Override
    public int operateData(CudRequest request) throws IOException
    {
        return 0;
    }

    @Override
    public int addData(CudRequest request) throws IOException
    {
        return 0;
    }

    @Override
    public int deleteData(CudRequest request) throws IOException
    {
        return 0;
    }

    @Override
    public int updateData(CudRequest request) throws IOException
    {
        return 0;
    }

    @Override
    public int batchAdd(List<CudRequest> requestList) throws IOException
    {
        return 0;
    }

    @Override
    public Long countData(String dbId,String esIndex) throws IOException
    {
        return null;
    }

    @Override
    public void checkAndCreateStorage(TableMeta tableMeta) throws IOException
    {

    }



    /**
     * 构建where的部分的sql语句，以主键为参数。
     * 在delete和update时可以用到。
     * @param tableMeta
     * @return
     */
    public String buildWhereConditionSqlByPks(TableMeta tableMeta){
        StringBuilder whereStr = new StringBuilder();
        List<String> pkNameList = tableMeta.getPrimaryKeyOnlyName();
        int i =0;
        //we must consider the situation of composite primary key
        for (String pkName:pkNameList) {
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
     * @param cudRequest
     * @return 返回一个数组，数组中的元素对应sql中的placeholder
     */
    public Object[] fillDeleteParam(CudRequest cudRequest)
    {
        TableMeta tableMeta = cudRequest.getTableMeta();
        Object [] params = new Object[tableMeta.getPrimaryKeyOnlyName().size()];
        int i = 0;
        for(String pkName:tableMeta.getPrimaryKeyOnlyName())
        {
            params[i] = cudRequest.getPkValueMap().get(pkName).getValue();
            i++;
        }
        return params;
    }

    /**
     * 构建insert sql语句
     * @param tableMeta
     * @return
     */
    public String buildInsertSqlByPks(TableMeta tableMeta){
        String insertColumns = tableMeta.getAllColumnList().stream()
                .map(field -> ColumnUtils.addEscape(field.getColumnName()))
                .collect(Collectors.joining(", "));
        String insertValues = tableMeta.getAllColumnList().stream().map(field -> "?")
                .collect(Collectors.joining(", "));

        return String.format(INSERT_SQL_TEMPLATE, tableMeta.getTableName(), insertColumns, insertValues);
    }

    /**
     * 为insert语句填充参数
     * @param cudRequest
     * @return 返回一个数组，数组中的元素对应sql中的placeholder
     */
    public Object[] fillInsertParam(CudRequest cudRequest)
    {
        TableMeta tableMeta = cudRequest.getTableMeta();
        Object [] params = new Object[tableMeta.getAllColumnList().size()];
        int i = 0;
        for(ColumnMeta columnMeta:tableMeta.getAllColumnList())
        {
            params[i] = cudRequest.getParameters().get(columnMeta.getColumnName());
            i++;
        }
        return params;
    }
}

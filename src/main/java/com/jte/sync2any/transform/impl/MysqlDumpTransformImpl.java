package com.jte.sync2any.transform.impl;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IORuntimeException;
import cn.hutool.core.io.IoUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.jte.sync2any.load.DynamicDataAssign;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.ColumnMeta;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.transform.DumpTransform;
import com.jte.sync2any.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static com.jte.sync2any.model.mq.SubscribeDataProto.DMLType.INSERT;

/**
 * 将从mysql导出的dumpfile文件转化为CudRequest
 */
@Service
@Slf4j
public class MysqlDumpTransformImpl implements DumpTransform {

    private final String SQL_START_FLAG = "INSERT INTO";

    @Override
    public Iterator transform(File file, TableMeta tableMeta) throws FileNotFoundException {
        FileRequest request = new FileRequest(file, this, tableMeta);
        return request;
    }

    private String getPkValueStr(SQLInsertStatement.ValuesClause values, TableMeta tableMeta) {
        StringBuilder docId = new StringBuilder();
        //主键字段名
        List<String> pkColumnName = tableMeta.getPrimaryKeyOnlyName()
                .stream()
                .sorted()
                .collect(Collectors.toList());;
        List<String> allColumnNameList = tableMeta.getAllColumnList()
                .stream()
                .map(c->c.getColumnName())
                .collect(Collectors.toList());

        List<SQLExpr> valueList = values.getValues();
        for (int i = 0; i < pkColumnName.size(); i++) {
            int valueIndex = allColumnNameList.indexOf(pkColumnName.get(i));
            if (i > 0) {
                docId.append("_");
            }
            docId.append(DbUtils.delQuote(valueList.get(valueIndex).toString()));
        }
        return docId.toString();
    }

    /**
     * 获取对应的参数值。key为列名，value为值
     *
     * @param values 对应一条记录
     * @return
     */
    private Map<String, Object> getInsertParameters(SQLInsertStatement.ValuesClause values, TableMeta tableMeta) {
        Map<String, Object> params = new HashMap<>(70);
        List<ColumnMeta> columnMetaList = tableMeta.getAllColumnList();
        List<SQLExpr> valueList = values.getValues();
        for (int i = 0; i < valueList.size(); i++) {
            SQLExpr currValue = valueList.get(i);
            ColumnMeta columnMeta = columnMetaList.get(i);
            if (columnMeta.isInclude()) {
                String value = currValue.toString();
                if ("NULL".equals(value) && Objects.isNull(currValue.computeDataType())) {
                    value = null;
                }
                params.put(columnMeta.getTargetColumnName(), DbUtils.delQuote(value));
            }
        }
        return params;
    }

    private List<CudRequest> sqlToInsertCudRequest(String line, TableMeta tableMeta) {
        List<CudRequest> requestList = new ArrayList<>();
        if (line.startsWith(SQL_START_FLAG)) {
            try {
                MySqlStatementParser parser = new MySqlStatementParser(line);
                SQLStatement statement = parser.parseStatement();
                MySqlInsertStatement insert = (MySqlInsertStatement) statement;

                for (SQLInsertStatement.ValuesClause values : insert.getValuesList()) {
                    CudRequest cudRequest = new CudRequest();
                    cudRequest.setPkValueStr(getPkValueStr(values, tableMeta));
                    cudRequest.setDmlType(INSERT);
                    Map<String, Object> parameterMap = getInsertParameters(values, tableMeta);
                    cudRequest.setParameters(parameterMap);
                    cudRequest.setTableMeta(tableMeta);
                    Object shardingValue = parameterMap.get(tableMeta.getShardingKey());
                    cudRequest.setTable(DynamicDataAssign.getDynamicTableName(shardingValue, tableMeta));
                    requestList.add(cudRequest);
                }
            } catch (Exception e) {
                //TODO告警
                log.error("parse sql error:{}", line, e);
            }
        }
        return requestList;
    }


    public class FileRequest implements Iterable, Iterator {
        /**
         * 要遍历的数据
         **/
        protected File sourceFile;
        protected MysqlDumpTransformImpl dt;
        protected TableMeta tableMeta;
        protected String currentLine;
        protected BufferedReader reader = null;


        public FileRequest(final File sourceFile, MysqlDumpTransformImpl dt, TableMeta tableMeta) throws FileNotFoundException {
            if (!sourceFile.exists()) {
                throw new FileNotFoundException(sourceFile.getPath());
            }
            this.dt = dt;
            this.sourceFile = sourceFile;
            this.tableMeta = tableMeta;
            setData();
        }

        /**
         * 设置（重置）数组为给定的数组，重置迭代器。
         * 参数d代表被迭代的数组对象。
         */

        public void setData() {
            try {
                reader = FileUtil.getReader(sourceFile, StandardCharsets.UTF_8);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public Iterator iterator() {
            setData();
            return this;
        }

        /**
         * 不可重复调用，因为调用该方法会导致下标移动
         * @return
         */
        @Override
        public boolean hasNext() {
            try {
                currentLine = reader.readLine();
                if (Objects.isNull(currentLine)) {
                    IoUtil.close(reader);
                    return false;
                }
                return true;
            } catch (IOException e) {
                IoUtil.close(reader);
                throw new IORuntimeException(e);
            }
        }

        @Override
        public List<CudRequest> next() {
            if (Objects.nonNull(currentLine)) {
                return dt.sqlToInsertCudRequest(currentLine, tableMeta);
            }
            IoUtil.close(reader);
            throw new NoSuchElementException("file is ended");
        }
    }
}

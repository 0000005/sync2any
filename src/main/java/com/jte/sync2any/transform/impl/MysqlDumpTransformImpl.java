package com.jte.sync2any.transform.impl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.ColumnMeta;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.transform.DumpTransform;
import com.jte.sync2any.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

import static com.jte.sync2any.model.mq.SubscribeDataProto.DMLType.INSERT;

/**
 * 将从mysql导出的dumpfile文件转化为CudRequest
 */
@Service
@Slf4j
public class MysqlDumpTransformImpl implements DumpTransform {

    private final String SQL_START_FLAG="INSERT INTO";

    @Override
    public Iterator transform(File file, TableMeta tableMeta) throws FileNotFoundException {
        FileEsRequest request = new FileEsRequest(file,this,tableMeta);
        return request;
    }

    private String getDocId(SQLInsertStatement.ValuesClause values, TableMeta tableMeta) {
        StringBuilder docId=new StringBuilder();
        //主键字段名
        List<String> pkColumnName=tableMeta.getPrimaryKeyOnlyName();
        List<ColumnMeta> pkColumnMetaList=tableMeta.getAllColumnList()
                .stream().filter(c->pkColumnName.contains(c.getColumnName()))
                .collect(Collectors.toList());
        List<SQLExpr> valueList=values.getValues();
        for(int i =0;i<pkColumnMetaList.size();i++)
        {
            int valueIndex=pkColumnName.indexOf(pkColumnMetaList.get(i).getColumnName());
            if(i>0)
            {
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
    private Map<String, Object> getInsertParameters(SQLInsertStatement.ValuesClause values, TableMeta tableMeta){
        Map<String, Object> params= new HashMap<>(70);
        List<ColumnMeta> columnMetaList=tableMeta.getAllColumnList();
        List<SQLExpr> valueList=values.getValues();
        for(int i =0;i<valueList.size();i++)
        {
            SQLExpr currValue=valueList.get(i);
            ColumnMeta columnMeta= columnMetaList.get(i);
            if(columnMeta.isInclude())
            {
                String value=currValue.toString();
                if("NULL".equals(value)&&Objects.isNull(currValue.computeDataType()))
                {
                    value=null;
                }
                params.put(columnMeta.getTargetColumnName(),DbUtils.delQuote(value));
            }
        }
        return params;
    }

    private List<CudRequest> sqlToInsertCudRequest(String line, TableMeta tableMeta)
    {
        List<CudRequest> requestList = new ArrayList<>();
        if(line.startsWith(SQL_START_FLAG))
        {
            MySqlStatementParser parser = new MySqlStatementParser(line);
            SQLStatement statement = parser.parseStatement();
            MySqlInsertStatement insert = (MySqlInsertStatement)statement;

            for(SQLInsertStatement.ValuesClause values:insert.getValuesList()){
                CudRequest cudRequest = new CudRequest();
                cudRequest.setPkValueStr(getDocId(values,tableMeta));
                cudRequest.setTable(tableMeta.getTargetTableName());
                cudRequest.setDmlType(INSERT);
                cudRequest.setParameters(getInsertParameters(values,tableMeta));
                cudRequest.setTableMeta(tableMeta);
                requestList.add(cudRequest);
            }
        }
        return requestList;
    }



    public class FileEsRequest implements Iterable, Iterator {
        /**
         * 要遍历的数据
         **/
        protected File sourceFile ;
        protected Scanner data ;
        protected MysqlDumpTransformImpl dt ;
        protected TableMeta tableMeta ;

        public FileEsRequest(final File sourceFile,MysqlDumpTransformImpl dt,TableMeta tableMeta) throws FileNotFoundException {
            if(!sourceFile.exists())
            {
                throw new FileNotFoundException(sourceFile.getPath());
            }
            this.dt=dt;
            this.sourceFile=sourceFile;
            this.tableMeta=tableMeta;
            setData();
        }

        /**
         * 设置（重置）数组为给定的数组，重置迭代器。
         * 参数d代表被迭代的数组对象。
         *
         */

        public void setData() {
            try
            {
                this.data = new Scanner(this.sourceFile);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        @Override
        public Iterator iterator() {
            setData();
            return this;
        }

        @Override
        public boolean hasNext() {
            boolean hasNext=data.hasNextLine();
            if(!hasNext)
            {
                this.data.close();
                FileUtils.deleteQuietly(sourceFile);
            }
            return hasNext;
        }

        @Override
        public List<CudRequest> next() {
            if (hasNext()) {
                return dt.sqlToInsertCudRequest(data.nextLine(),tableMeta);
            }
            throw new NoSuchElementException("file is ended");
        }
    }
}
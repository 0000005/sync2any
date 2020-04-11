package com.jte.sync2es.transform.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.jte.sync2es.model.es.EsRequest;
import com.jte.sync2es.model.mysql.ColumnMeta;
import com.jte.sync2es.model.mysql.TableMeta;
import com.jte.sync2es.transform.DumpTransform;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import static com.jte.sync2es.extract.impl.KafkaMsgListener.EVENT_TYPE_INSERT;

@Service
@Slf4j
public class MysqlDumpTransformImpl implements DumpTransform {

    private final String SQL_START_FLAG="INSERT INTO";

    @Override
    public FileEsRequest transform(File file, TableMeta tableMeta) throws FileNotFoundException {
        FileEsRequest request = new FileEsRequest(file,this,tableMeta);
        return request;
    }

    private String getDocId(SQLInsertStatement.ValuesClause values, TableMeta tableMeta) {

        return null;
    }

    /**
     * 获取对应的参数值。key为列名，value为值
     *
     * @param values 对应es的一条记录
     * @return
     */
    private Map<String, Object> getParameters(SQLInsertStatement.ValuesClause values, TableMeta tableMeta){
        Map<String, Object> params= new HashMap<>(70);
        Map<String, ColumnMeta> columnMetaMap=tableMeta.getAllColumnMap();
//        for(String columnName:values.getValues())
//        {
//            ColumnMeta columnMeta=columnMetaMap.get(columnName);
//            if(columnMeta.isInclude())
//            {
//                params.put(columnMeta.getEsColumnName(),pkRow.get(columnName).getValue());
//            }
//        }
        return params;
    }

    private List<EsRequest> sqlToEsRequest(String line, TableMeta tableMeta)
    {
        List<EsRequest> requestList = new ArrayList<>();
        if(line.startsWith(SQL_START_FLAG))
        {
            MySqlStatementParser parser = new MySqlStatementParser(line);
            SQLStatement statement = parser.parseStatement();
            MySqlInsertStatement insert = (MySqlInsertStatement)statement;

            for(SQLInsertStatement.ValuesClause values:insert.getValuesList()){
                EsRequest esRequest = new EsRequest();
                esRequest.setDocId(getDocId(values,tableMeta));
                esRequest.setIndex(tableMeta.getEsIndexName());
                esRequest.setOperationType(EVENT_TYPE_INSERT);
                esRequest.setParameters(getParameters(values,tableMeta));
                esRequest.setTableMeta(tableMeta);
                requestList.add(esRequest);
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
            return data.hasNextLine();
        }

        @Override
        public List<EsRequest> next() {
            if (hasNext()) {
                return dt.sqlToEsRequest(data.nextLine(),tableMeta);
            }
            throw new NoSuchElementException("file is ended");
        }
    }
}

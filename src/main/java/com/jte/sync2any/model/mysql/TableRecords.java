/*

 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.jte.sync2any.model.mysql;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.model.mq.SubscribeDataProto;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.*;

/**
 * The type Table records.
 *
 * @author sharajava
 */
@Slf4j
public class TableRecords {

    /**
     * 拼凑TableRecords需要分两种情况处理
     */
    private static String TYPE_OLD ="old";
    private static String TYPE_NEW ="new";

    private transient TableMeta tableMeta;

    /**
     * dml类型
     */
    private SubscribeDataProto.DMLEvent dmlEvt;

    private String tableName;

    /**
     * 对应binlog修改前的行
     */
    private List<Row> oldRows = new ArrayList<>();
    /**
     * 对应binlog修改后的行
     */
    private List<Row> newRows = new ArrayList<>();


    /**
     * Gets table name.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets table name.
     *
     * @param tableName the table name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Gets whereRows.
     *
     * @return the whereRows
     */
    public List<Row> getOldRows() {
        return oldRows;
    }

    /**
     * Sets whereRows.
     *
     * @param oldRows the whereRows
     */
    public void setOldRows(List<Row> oldRows) {
        this.oldRows = oldRows;
    }

    public List<Row> getNewRows() {
        return newRows;
    }

    public void setNewRows(List<Row> newRows) {
        this.newRows = newRows;
    }

    /**
     * Instantiates a new Table records.
     */
    private TableRecords() {

    }

    /**
     * Instantiates a new Table records.
     *
     * @param tableMeta the table meta
     */
    public TableRecords(TableMeta tableMeta) {
        setTableMeta(tableMeta);
    }

    /**
     * Sets table meta.
     *
     * @param tableMeta the table meta
     */
    public void setTableMeta(TableMeta tableMeta) {
        if (this.tableMeta != null) {
            throw new ShouldNeverHappenException();
        }
        this.tableMeta = tableMeta;
        this.tableName = tableMeta.getTableName();
    }



    /**
     * Add where row.
     *
     * @param row the row
     */
    public void addOldRow(Row row) {
        oldRows.add(row);
    }

    /**
     * Add field row.
     *
     * @param row the row
     */
    public void addNewRow(Row row) {
        newRows.add(row);
    }

    /**
     *  Get primary key value
     * @param rows 传入whereRows 还是 fieldRows
     * @return return a list. each element of list is a map as a row,the map hold the pk column name as a key and field as the value
     */
    public List<Map<String,Field>> pkRows(List<Row> rows) {
        final List<String> pkNameList = getTableMeta().getPrimaryKeyOnlyName();
        List<Map<String,Field>> pkRows = new ArrayList<>();
        for (Row row : rows) {
            List<Field> fields = row.getFields();
            Map<String,Field> rowMap = new HashMap<>(3);
            for (Field field : fields) {
                if (pkNameList.stream().anyMatch(e -> field.getName().equalsIgnoreCase(e))) {
                    rowMap.put(field.getName(),field);
                }
            }
            pkRows.add(rowMap);
        }
        return pkRows;
    }

    /**
     *  Get all column's key value
     * @param rows 传入whereRows 还是 fieldRows
     * @return return a list. each element of list is a map as a row,the map hold the pk column name as a key and field as the value
     */
    public List<Map<String,Field>> parseToMap(List<Row> rows) {
        List<Map<String,Field>> pkRows = new ArrayList<>();
        for (Row row : rows) {
            Map<String,Field> rowMap = new HashMap<>(70);
            for (Field field : row.getFields()) {
                rowMap.put(field.getName(),field);
            }
            pkRows.add(rowMap);
        }
        return pkRows;
    }

    /**
     * Gets table meta.
     *
     * @return the table meta
     */
    public TableMeta getTableMeta() {
        return tableMeta;
    }


    /**
     * Build records table records.
     *
     * @param tableMeta     the tmeta
     * @param row    the result set
     * @param dmlEvt    语句类型
     * @return the table records
     * @throws SQLException the sql exception
     */
    public static TableRecords buildRecords(TableMeta tableMeta, SubscribeDataProto.RowChange row,SubscribeDataProto.DMLEvent dmlEvt) {
        if(Objects.isNull(tableMeta))
        {
            throw new IllegalArgumentException("TableMeta is null");
        }
        TableRecords records = new TableRecords(tableMeta);
        records.setTableName(tableMeta.getTableName());
        records.setDmlEvt(dmlEvt);


        List<Field> oldFields = new ArrayList<>(row.getOldColumnsCount());
        assembleFields(tableMeta,dmlEvt,oldFields,row,TYPE_OLD);
        if(!oldFields.isEmpty()){
            Row whereRow = new Row();
            whereRow.setFields(oldFields);
            records.addOldRow(whereRow);
        }

        List<Field> newFields = new ArrayList<>(row.getNewColumnsCount());
        assembleFields(tableMeta,dmlEvt,newFields,row,TYPE_NEW);
        if(!newFields.isEmpty()){
            Row fieldRow = new Row();
            fieldRow.setFields(newFields);
            records.addNewRow(fieldRow);
        }

        return records;
    }

    /**
     * 拼装fields
     * @param tableMeta
     * @param dmlEvt
     * @param fields
     * @param row
     * @param type
     */
    private static void assembleFields(TableMeta tableMeta, SubscribeDataProto.DMLEvent dmlEvt ,List<Field> fields,
                                SubscribeDataProto.RowChange row,String type){
        int columnCount=0;
        if(TYPE_OLD.equals(type))
        {
            columnCount = row.getOldColumnsCount();
        }
        else if(TYPE_NEW.equals(type))
        {
            columnCount = row.getNewColumnsCount();
        }
        for (int i = 0; i < columnCount; i++) {
            SubscribeDataProto.Data col =null;
            if(TYPE_OLD.equals(type))
            {
                col = row.getOldColumns(i);
            }
            else if(TYPE_NEW.equals(type))
            {
                col = row.getNewColumns(i);
            }

            if (col.getDataType() == SubscribeDataProto.DataType.NA)
            {
                continue;
            }
            SubscribeDataProto.Column colDef = dmlEvt.getColumns(i);
            String columnName = colDef.getName().toLowerCase();
            ColumnMeta currColumnMeta = tableMeta.getColumnMeta(columnName);
            //TODO 如果更新了表结构，那么这个地方可能找不到最新的字段
            if(currColumnMeta==null || !currColumnMeta.isInclude()){
                //该字段从配置文件中排除了
                continue;
            }
            try{
                log.debug("field sourceDataType1:{} sourceDataType2:{} sourceDataTypeName:{} name:{} value:{}"
                        ,col.getDataType(),currColumnMeta.getDataType()
                        ,currColumnMeta.getDataTypeName(),currColumnMeta.getColumnName()
                        ,decode(col));
                Field newField = createFieldValue(decode(col),currColumnMeta,tableMeta);
                fields.add(newField);
            }catch (UnsupportedEncodingException e){
                throw new ShouldNeverHappenException("UnsupportedEncodingException "+e.getMessage());
            }

        }
    }

    private static Field createFieldValue(String value,ColumnMeta columnMeta,TableMeta meta){
        Field field = new Field();
        field.setName(columnMeta.getColumnName());
        if (meta.getPrimaryKeyOnlyName().stream().anyMatch(e -> field.getName().equalsIgnoreCase(e))) {
            field.setKeyType(KeyType.PRIMARY_KEY);
        }
        field.setType(columnMeta.getDataType());
        if(Objects.nonNull(value) && "TIMESTAMP".equalsIgnoreCase(columnMeta.getDataTypeName())){
            DateTime dateTime = new DateTime(value, DatePattern.NORM_DATETIME_FORMAT);
            dateTime.offset(DateField.HOUR,8);
            field.setValue(dateTime.toString());
        }else {
            field.setValue(value);
        }
        return field;
    }


    private static String decode(SubscribeDataProto.Data data) throws UnsupportedEncodingException {
        switch (data.getDataType()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case UINT8:
            case UINT16:
            case UINT32:
            case UINT64:
            case DECIMAL:
            case FLOAT32:
            case FLOAT64:
                return data.getSv();
            case STRING:
                return new String(data.getBv().toByteArray());
            case BYTES:
                return data.getBv().toString("utf-8");
            case NA:
                return "DEFAULT";
            case NIL:
                return null;
            default:
                throw new IllegalStateException("unsupported data type");
        }
    }

    public SubscribeDataProto.DMLEvent getDmlEvt() {
        return dmlEvt;
    }

    public void setDmlEvt(SubscribeDataProto.DMLEvent dmlEvt) {
        this.dmlEvt = dmlEvt;
    }
}

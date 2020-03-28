/*
 *  Copyright 1999-2019 Seata.io Group.
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
package com.jte.sync2es.model.mysql;

import com.jte.sync2es.conf.KafkaConfig;
import com.jte.sync2es.exception.IllegalDataStructureException;
import com.jte.sync2es.exception.ShouldNeverHappenException;
import com.jte.sync2es.extract.impl.KafkaMsgListener;
import com.jte.sync2es.transform.RecordsTransform;
import com.jte.sync2es.model.mq.TcMqMessage;
import com.jte.sync2es.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;

/**
 * The type Table records.
 *
 * @author sharajava
 */
@Slf4j
public class TableRecords {

    private transient TableMeta tableMeta;

    private String tableName;

    /**
     * 对应mq消息中的where属性
     */
    private List<Row> whereRows = new ArrayList<Row>();
    /**
     * 对应mq消息中的field属性
     */
    private List<Row> fieldRows = new ArrayList<Row>();

    private TcMqMessage mqMessage;

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
    public List<Row> getWhereRows() {
        return whereRows;
    }

    /**
     * Sets whereRows.
     *
     * @param whereRows the whereRows
     */
    public void setWhereRows(List<Row> whereRows) {
        this.whereRows = whereRows;
    }

    public TcMqMessage getMqMessage() {
        return mqMessage;
    }

    public void setMqMessage(TcMqMessage mqMessage) {
        this.mqMessage = mqMessage;
    }

    public List<Row> getFieldRows() {
        return fieldRows;
    }

    public void setFieldRows(List<Row> fieldRows) {
        this.fieldRows = fieldRows;
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
    public void addWhereRow(Row row) {
        whereRows.add(row);
    }

    /**
     * Add field row.
     *
     * @param row the row
     */
    public void addFieldRow(Row row) {
        fieldRows.add(row);
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
     * @param meta     the tmeta
     * @param mqMessage the result set
     * @return the table records
     * @throws SQLException the sql exception
     */
    public static TableRecords buildRecords(TableMeta meta, TcMqMessage mqMessage) throws IllegalDataStructureException {
        if(Objects.isNull(meta))
        {
            throw new IllegalArgumentException("TableMeta is null");
        }
        TableRecords records = new TableRecords(meta);
        records.setTableName(meta.getTableName());
        records.setMqMessage(mqMessage);
        List<ColumnMeta> columnMetaList=meta.getAllColumnList();
        int tableColumnSize=columnMetaList.size();
        int fieldSize=mqMessage.getField().size();
        int whereSize=mqMessage.getWhere().size();

        if(KafkaMsgListener.EVENT_TYPE_DELETE.equals(mqMessage.getEventtypestr())&&whereSize!=tableColumnSize)
        {
            log.error("meta:{} mqMessage:{}",JsonUtil.objectToJson(meta),JsonUtil.objectToJson(mqMessage));
            throw new IllegalDataStructureException("column can't match when delete! tableName:"+meta.getTableName());
        }
        else if(KafkaMsgListener.EVENT_TYPE_INSERT.equals(mqMessage.getEventtypestr())&&fieldSize!=tableColumnSize)
        {
            log.error("meta:{} mqMessage:{}",JsonUtil.objectToJson(meta),JsonUtil.objectToJson(mqMessage));
            throw new IllegalDataStructureException("column can't match when delete! tableName:"+meta.getTableName());
        }
        else if(KafkaMsgListener.EVENT_TYPE_UPDATE.equals(mqMessage.getEventtypestr())&&
                (fieldSize!=tableColumnSize||whereSize!=tableColumnSize))
        {
            log.error("meta:{} mqMessage:{}",JsonUtil.objectToJson(meta),JsonUtil.objectToJson(mqMessage));
            throw new IllegalDataStructureException("column can't match when delete! tableName:"+meta.getTableName());
        }

        boolean shouldCalculateWhere=KafkaMsgListener.EVENT_TYPE_DELETE.equals(mqMessage.getEventtypestr())||KafkaMsgListener.EVENT_TYPE_UPDATE.equals(mqMessage.getEventtypestr());
        boolean shouldCalculateField=KafkaMsgListener.EVENT_TYPE_INSERT.equals(mqMessage.getEventtypestr())||KafkaMsgListener.EVENT_TYPE_UPDATE.equals(mqMessage.getEventtypestr());

        List<Field> whereFields = new ArrayList<>(tableColumnSize);
        List<Field> fieldFields = new ArrayList<>(tableColumnSize);
        for(int i =0;i<columnMetaList.size();i++)
        {
            ColumnMeta currColumn=columnMetaList.get(i);

            if(shouldCalculateWhere)
            {
                String whereValue=mqMessage.getWhere().get(i);
                //对应mq中的where属性
                Field whereField = new Field();
                whereField.setName(currColumn.getColumnName());
                if (meta.getPrimaryKeyOnlyName().stream().anyMatch(e -> whereField.getName().equalsIgnoreCase(e))) {
                    whereField.setKeyType(KeyType.PRIMARY_KEY);
                }
                whereField.setType(currColumn.getDataType());
                whereField.setValue(whereValue);
                whereFields.add(whereField);
            }

            if(shouldCalculateField)
            {
                String fieldValue=mqMessage.getField().get(i);
                //对应mq中的field属性
                Field fieldField = new Field();
                fieldField.setName(currColumn.getColumnName());
                if (meta.getPrimaryKeyOnlyName().stream().anyMatch(e -> fieldField.getName().equalsIgnoreCase(e))) {
                    fieldField.setKeyType(KeyType.PRIMARY_KEY);
                }
                fieldField.setType(currColumn.getDataType());
                fieldField.setValue(fieldValue);
                fieldFields.add(fieldField);
            }
        }

        if(shouldCalculateWhere)
        {
            Row whereRow = new Row();
            whereRow.setFields(whereFields);
            records.addWhereRow(whereRow);
        }

        if(shouldCalculateField)
        {
            Row fieldRow = new Row();
            fieldRow.setFields(fieldFields);
            records.addFieldRow(fieldRow);
        }
        return records;
    }


}

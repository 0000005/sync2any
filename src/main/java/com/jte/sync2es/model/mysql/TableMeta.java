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


import com.jte.sync2es.model.core.SyncState;

import java.util.*;
import java.util.Map.Entry;

/**
 * The type Table meta.
 *
 * @author sharajava
 */
public class TableMeta {
    private String tableName;
    private String esIndexName;
    private String dbName;
    /**
     * kafka topic name
     */
    private String topicName;
    /**
     * kafka group name
     */
    private String topicGroup;

    /**
     * the local time of last sync
     */
    private long lastSyncTime=0;

    /**
     * this time is refer to the update time of source data(tdsql).
     * delayTime = lastSyncTime - lastDataManipulateTime
     */
    private long lastDataManipulateTime=0;

    /**
     *
     * 0: waiting to start
     * 1: sync is stopped
     * 2: loading origin data
     * 3: sync is running
     */
    private SyncState state=SyncState.WAITING;

    /**
     * key: column name
     */
    private Map<String, ColumnMeta> allColumnMap = new LinkedHashMap<String, ColumnMeta>();
    /**
     * list of column,make sure the order of item.
     */
    private List<ColumnMeta> allColumnList = new ArrayList();
    /**
     * key: index name
     */
    private Map<String, IndexMeta> allIndexes = new LinkedHashMap<String, IndexMeta>();


    public SyncState getState() {
        return state;
    }

    public void setState(SyncState state) {
        this.state = state;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicGroup() {
        return topicGroup;
    }

    public void setTopicGroup(String topicGroup) {
        this.topicGroup = topicGroup;
    }

    public long getLastSyncTime() {
        return lastSyncTime;
    }

    public void setLastSyncTime(long lastSyncTime) {
        this.lastSyncTime = lastSyncTime;
    }

    public long getLastDataManipulateTime() {
        return lastDataManipulateTime;
    }

    public void setLastDataManipulateTime(long lastDataManipulateTime) {
        this.lastDataManipulateTime = lastDataManipulateTime;
    }

    public String getEsIndexName() {
        return esIndexName;
    }

    public void setEsIndexName(String esIndexName) {
        this.esIndexName = esIndexName;
    }

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
     * Gets column meta.
     *
     * @param colName the col name
     * @return the column meta
     */
    public ColumnMeta getColumnMeta(String colName) {
        return allColumnMap.get(colName);
    }

    /**
     * Gets all columns.
     *
     * @return the all columns
     */
    public Map<String, ColumnMeta> getAllColumnMap() {
        return allColumnMap;
    }

    /**
     * Gets all indexes.
     *
     * @return the all indexes
     */
    public Map<String, IndexMeta> getAllIndexes() {
        return allIndexes;
    }


    public List<ColumnMeta> getAllColumnList() {
        return allColumnList;
    }

    public void setAllColumnList(List<ColumnMeta> allColumnList) {
        this.allColumnList = allColumnList;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * Gets primary key map.
     *
     * @return the primary key map
     */
    public Map<String, ColumnMeta> getPrimaryKeyMap() {
        Map<String, ColumnMeta> pk = new HashMap<String, ColumnMeta>();
        for (Entry<String, IndexMeta> entry : allIndexes.entrySet()) {
            IndexMeta index = entry.getValue();
            if (index.getIndextype().value() == IndexType.PRIMARY.value()) {
                for (ColumnMeta col : index.getValues()) {
                    pk.put(col.getColumnName(), col);
                }
            }
        }
        return pk;
    }

    /**
     * Gets primary key only name.
     *
     * @return the primary key only name
     */
    @SuppressWarnings("serial")
    public List<String> getPrimaryKeyOnlyName() {
        List<String> list = new ArrayList<>();
        for (Entry<String, ColumnMeta> entry : getPrimaryKeyMap().entrySet()) {
            list.add(entry.getKey().toLowerCase());
        }
        return list;
    }



    /**
     * Contains pk boolean.
     *
     * @param cols the cols
     * @return the boolean
     */
    public boolean containsPK(List<String> cols) {
        if (cols == null) {
            return false;
        }

        List<String> pk = getPrimaryKeyOnlyName();
        if (pk.isEmpty()) {
            return false;
        }
        //at least contain one pk
        if (cols.containsAll(pk)) {
            return true;
        } else {
            return cols.containsAll(pk);
        }
    }

}

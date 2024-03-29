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


import com.jte.sync2any.model.config.SyncConfig;
import com.jte.sync2any.model.core.SyncState;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.Map.Entry;

/**
 * The type Table meta.
 *
 * @author sharajava
 */
public class TableMeta {
    private SyncConfig syncConfig;
    private String tableName;
    /**
     * 同步到目标表的名称
     */
    private String targetTableName;
    private String dbName;
    private String sourceDbId;
    private String targetDbId;
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
     * time per process
     */
    private long tpq;

    /**
     * last kafka offset
     */
    private long lastOffset;

    /**
     *  error reason for stop sync
     */
    private String errorReason;

    /**
     * 当前同步状态
     */
    private SyncState state=SyncState.INACTIVE;

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

    private long lastAlarmTime=0;

    private String dynamicTablenameAssigner;

    private String shardingKey;
    private String ckTableEngine;


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

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public long getTpq() {
        return tpq;
    }

    public void setTpq(long tpq) {
        this.tpq = tpq;
    }

    public String getErrorReason() {
        if(StringUtils.isBlank(errorReason))
        {
            return errorReason;
        }
        else
        {
            return errorReason.replaceAll("\"","”").replaceAll("'","’");
        }
    }

    public void setErrorReason(String errorReason) {
        this.errorReason = errorReason;
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

    public long getLastAlarmTime() {
        return lastAlarmTime;
    }

    public void setLastAlarmTime(long lastAlarmTime) {
        this.lastAlarmTime = lastAlarmTime;
    }

    public String getSourceDbId() {
        return sourceDbId;
    }

    public void setSourceDbId(String sourceDbId) {
        this.sourceDbId = sourceDbId;
    }

    public String getTargetDbId() {
        return targetDbId;
    }

    public void setTargetDbId(String targetDbId) {
        this.targetDbId = targetDbId;
    }

    public String getDynamicTablenameAssigner() {
        return dynamicTablenameAssigner;
    }

    public void setDynamicTablenameAssigner(String dynamicTablenameAssigner) {
        this.dynamicTablenameAssigner = dynamicTablenameAssigner;
    }

    public String getShardingKey() {
        return shardingKey;
    }

    public void setShardingKey(String shardingKey) {
        this.shardingKey = shardingKey;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public SyncConfig getSyncConfig() {
        return syncConfig;
    }

    public void setSyncConfig(SyncConfig syncConfig) {
        this.syncConfig = syncConfig;
    }

    public String getCkTableEngine() {
        return ckTableEngine;
    }

    public void setCkTableEngine(String ckTableEngine) {
        this.ckTableEngine = ckTableEngine;
    }
}

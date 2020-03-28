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

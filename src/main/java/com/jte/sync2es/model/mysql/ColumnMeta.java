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
package com.jte.sync2es.model.mysql;

import lombok.Data;

/**
 * The type Column meta.
 *
 * @author sharajava
 */
@Data
public class ColumnMeta {
    private String tableCat;
    private String tableSchemaName;
    private String tableName;
    private String columnName;
    private int dataType;
    private String dataTypeName;
    private String esDataType;
    private String esColumnName;
    private int columnSize;
    private int decimalDigits;
    private int numPrecRadix;
    private int nullAble;
    private String remarks;
    private String columnDef;
    private int sqlDataType;
    private int sqlDatetimeSub;
    private int charOctetLength;
    private int ordinalPosition;
    private String isNullAble;
    private String isAutoincrement;
    /**
     * 是否生效（由配置field-filter决定）；默认为true
     */
    private boolean isInclude=true;
}

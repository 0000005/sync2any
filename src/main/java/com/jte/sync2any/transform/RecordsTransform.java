package com.jte.sync2any.transform;

import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mq.SubscribeDataProto;
import com.jte.sync2any.model.mysql.ColumnMeta;
import com.jte.sync2any.model.mysql.Field;
import com.jte.sync2any.model.mysql.TableRecords;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.jte.sync2any.model.mq.SubscribeDataProto.DMLType.*;

/**
 * 用于解析mq收到的消息
 */
public abstract class RecordsTransform {
    /**
     * 将TableRecords转换为目标数据库可使用的中间类型。
     * <p>
     * 考虑到MQ中的数据有“毛刺”，且数据库结构的变更在MQ消息中并不能明显的体现出值对应的字段。
     * 我们规定：
     * 1、当往数据库加字段时，必须将字段加到最末尾，切记不可将新字段插入旧的字段前。
     * 2、不支持删除字段。 //TODO 可以支持，需要新增一个fallback的变量保存之前的表结构。每10分钟只能删一个字段
     * 基于以上规定，我们才能够在数据库更改表结构的同时，平滑的同步数据。
     * 平滑同步数据的关键在于原数据源的字段与目标数据源的字段匹配。
     * 当原数据源新增字段时：
     * 由于“毛刺”现象的原因，MQ中会存在同时存在（无序）新表结构的数据，和旧表结构的数据。
     * 一旦发现MQ数据中字段数大于本地缓存表结构的字段数，则马上同步一次数据库结构（可能拉不到最新的表结构，会尝试3次）
     * 接下来按照最新的表结构来处理MQ中的数据。按照字段顺序匹配，如若匹配不上，则抛弃没匹配上的字段。保证至少匹配上的字段是正确的。
     * <p>
     * 问题：
     * 1、假设字段没加到最后怎么处理？
     * 2、假设误删了字段怎么处理？
     *
     * @param records
     * @return
     */
    public abstract CudRequest transform(TableRecords records);


    /**
     * 获取对应的参数值。key为列名，value为值
     * 当是delete时，应该获取修改前的参数。因为要根据条件（主键）删除数据
     * 当是update\insert时，应该是修改后的参数。因为要修改（根据主键）或插入数据。
     *
     * @param records 对应数据库的一条记录
     * @return
     */
    protected Map<String, Object> getParameters(TableRecords records) {
        Map<String, Object> params = new HashMap<>(70);

        List<Map<String, Field>> rows = new ArrayList<>();
        SubscribeDataProto.DMLType dmlType = records.getDmlEvt().getDmlEventType();
        if (dmlType.equals(DELETE)) {
            //以修改前的数据为准
            rows = records.pkRows(records.getOldRows());
        } else if (dmlType.equals(INSERT) || dmlType.equals(UPDATE)) {
            //以修改后的数据为准
            rows = records.parseToMap(records.getNewRows());
        }
        if (rows.isEmpty()) {
            throw new ShouldNeverHappenException("can't find parameters!");
        }
        Map<String, Field> currRow = rows.get(0);
        Map<String, ColumnMeta> columnMetaMap = records.getTableMeta().getAllColumnMap();
        for (String columnName : currRow.keySet()) {
            ColumnMeta columnMeta = columnMetaMap.get(columnName);
            params.put(columnMeta.getTargetColumnName(), currRow.get(columnName).getValue());
        }
        return params;
    }

    /**
     * 以Map的形式获取主键值
     *
     * @param records
     * @return
     */
    protected Map<String, Field> getPkValueMap(TableRecords records) {
        SubscribeDataProto.DMLType dmlType = records.getDmlEvt().getDmlEventType();
        List<Map<String, Field>> rows = new ArrayList<>();
        if (dmlType.equals(DELETE) || dmlType.equals(UPDATE)) {
            //以where为主
            rows = records.pkRows(records.getOldRows());
        } else if (dmlType.equals(INSERT)) {
            //以field为主
            rows = records.pkRows(records.getNewRows());
        }
        if (rows.isEmpty()) {
            throw new ShouldNeverHappenException("row is empty");
        }

        //目前只考虑1行的情况
        Map<String, Field> pkRow = rows.get(0);
        return pkRow;
    }


    /**
     * 以字符串的形式获取主键值
     *
     * @param records
     * @return
     */
    protected String getPkValueStr(TableRecords records) {
        StringBuilder pkValueStr = new StringBuilder();
        Map<String, Field> pkRow = getPkValueMap(records);
        int index = 0;
        List<String> nameList = new ArrayList<>();
        nameList.addAll(pkRow.keySet());
        nameList = nameList.stream().sorted().collect(Collectors.toList());
        for (String columnName : nameList) {
            if (index > 0) {
                pkValueStr.append("_");
            }
            pkValueStr.append(pkRow.get(columnName).getValue());
            index++;
        }
        return pkValueStr.toString();
    }

}

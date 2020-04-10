package com.jte.sync2es.transform.impl;

import com.jte.sync2es.exception.ShouldNeverHappenException;
import com.jte.sync2es.extract.impl.KafkaMsgListener;
import com.jte.sync2es.model.es.EsRequest;
import com.jte.sync2es.model.mysql.ColumnMeta;
import com.jte.sync2es.model.mysql.Field;
import com.jte.sync2es.model.mysql.TableRecords;
import com.jte.sync2es.transform.RecordsTransform;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class RecordsTransform4EsImpl implements RecordsTransform {

    @Override
    public EsRequest transform(TableRecords records){
        EsRequest esRequest = new EsRequest();
        esRequest.setDocId(getDocId(records));
        esRequest.setIndex(records.getTableMeta().getEsIndexName());
        esRequest.setOperationType(records.getMqMessage().getEventtypestr());
        esRequest.setParameters(getParameters(records));
        esRequest.setTableMeta(records.getTableMeta());
        return esRequest;
    }

    /**
     * 获取对应的参数值。
     * 当是delete时，参数值应该获取where部分。因为要根据条件（主键）删除数据
     * 当是update\insert时，参数应该是field部分。因为要修改（根据主键）或插入数据。
     *
     * @param records 对应es的一条记录
     * @return
     */
    private Map<String, Object> getParameters(TableRecords records){
        Map<String, Object> params= new HashMap<>(70);

        List<Map<String,Field>> rows = new ArrayList<>();
        String eventTypeStr=records.getMqMessage().getEventtypestr();if(KafkaMsgListener.EVENT_TYPE_DELETE.equalsIgnoreCase(eventTypeStr))
        {
            //以where为主
            rows = records.pkRows(records.getWhereRows());
        }
        else if(KafkaMsgListener.EVENT_TYPE_UPDATE.equalsIgnoreCase(eventTypeStr)||
                KafkaMsgListener.EVENT_TYPE_INSERT.equalsIgnoreCase(eventTypeStr))
        {
            //以field为主
            rows = records.parseToMap(records.getFieldRows());
        }
        if(rows.isEmpty())
        {
            throw new ShouldNeverHappenException("can't find parameters!");
        }
        Map<String,Field> pkRow=rows.get(0);
        Map<String, ColumnMeta> columnMetaMap=records.getTableMeta().getAllColumnMap();
        for(String columnName:pkRow.keySet())
        {
            ColumnMeta columnMeta=columnMetaMap.get(columnName);
            if(columnMeta.isInclude())
            {
                params.put(columnMeta.getEsColumnName(),pkRow.get(columnName).getValue());
            }
        }
        return params;
    }

    /**
     * documentId由主键组成
     * @param records
     * @return
     */
    private String getDocId(TableRecords records)
    {
        StringBuilder docId=new StringBuilder();
        List<Map<String,Field>> rows = new ArrayList<>();
        String eventTypeStr=records.getMqMessage().getEventtypestr();
        if(KafkaMsgListener.EVENT_TYPE_DELETE.equalsIgnoreCase(eventTypeStr)||
                KafkaMsgListener.EVENT_TYPE_UPDATE.equalsIgnoreCase(eventTypeStr))
        {
            //以where为主
            rows = records.pkRows(records.getWhereRows());
        }
        else if(KafkaMsgListener.EVENT_TYPE_INSERT.equalsIgnoreCase(eventTypeStr))
        {
            //以field为主
            rows = records.pkRows(records.getFieldRows());
        }
        if(rows.isEmpty())
        {
            throw new ShouldNeverHappenException("can't determine docId!");
        }

        Map<String, ColumnMeta> columnMetaMap=records.getTableMeta().getAllColumnMap();
        //目前只考虑1行的情况
        Map<String,Field> pkRow=rows.get(0);
        int index=0;
        for(String columnName:pkRow.keySet())
        {
            ColumnMeta columnMeta=columnMetaMap.get(columnName);
            if(!columnMeta.isInclude())
            {
                continue;
            }
            if(index>0)
            {
                docId.append("_");
            }
            docId.append(pkRow.get(columnName).getValue());
            index++;
        }
        return docId.toString();
    }


}

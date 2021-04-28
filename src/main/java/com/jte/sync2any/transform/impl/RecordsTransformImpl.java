package com.jte.sync2any.transform.impl;

import com.jte.sync2any.load.DynamicDataAssign;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.TableRecords;
import com.jte.sync2any.transform.RecordsTransform;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RecordsTransformImpl extends RecordsTransform {

    @Override
    public CudRequest transform(TableRecords records){
        CudRequest cudRequest = new CudRequest();
        cudRequest.setPkValueStr(getPkValueStr(records));
        cudRequest.setPkValueMap(getPkValueMap(records));
        cudRequest.setDmlType(records.getDmlEvt().getDmlEventType());
        cudRequest.setParameters(getParameters(records));
        cudRequest.setTableMeta(records.getTableMeta());
        cudRequest.setTable(DynamicDataAssign.getDynamicTableName(records,records.getTableMeta()));
        return cudRequest;
    }





}

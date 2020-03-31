package com.jte.sync2es.load.es;

import com.jte.sync2es.exception.ShouldNeverHappenException;
import com.jte.sync2es.extract.impl.KafkaMsgListener;
import com.jte.sync2es.load.LoadService;
import com.jte.sync2es.model.es.EsRequest;
import com.jte.sync2es.model.mysql.TableMeta;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;

@Slf4j
@Service
public class EsLoadServiceImpl implements LoadService {

    @Resource
    RestHighLevelClient client;

    @Override
    public int operateData(EsRequest request) throws IOException {
        checkAndCreateStorage(request);
        if(KafkaMsgListener.EVENT_TYPE_INSERT.equalsIgnoreCase(request.getOperationType()))
        {
            return addData(request);
        }
        else if(KafkaMsgListener.EVENT_TYPE_UPDATE.equalsIgnoreCase(request.getOperationType()))
        {
            return updateData(request);
        }
        else if(KafkaMsgListener.EVENT_TYPE_DELETE.equalsIgnoreCase(request.getOperationType()))
        {
            return deleteData(request);
        }
        else
        {
            throw new ShouldNeverHappenException("unknown operation type:"+request.getOperationType());
        }
    }

    @Override
    public int addData(EsRequest request) throws IOException {

        return 0;
    }

    @Override
    public int deleteData(EsRequest request) throws IOException {
        return 0;
    }

    @Override
    public int updateData(EsRequest request) throws IOException {
        return 0;
    }

    /**
     * index命名规则：dbName-tableName
     * @param request
     * @return
     */
    @Override
    public void checkAndCreateStorage(EsRequest request) throws IOException {
        GetIndexRequest indexRequest = new GetIndexRequest(request.getIndex());
        TableMeta tableMeta=request.getTableMeta();
        boolean indexExists=client.indices().exists(indexRequest,RequestOptions.DEFAULT);
        if(!indexExists)
        {
            //不存在，创建映射关系
            PutMappingRequest newMapping = new PutMappingRequest();
            log.info("add new index mapping fro {}",request.getIndex());

        }
        else
        {
            LOAD_STORAGE.add("es$"+tableMeta.getDbName()+"$"+tableMeta.getTableName());
        }
    }


}

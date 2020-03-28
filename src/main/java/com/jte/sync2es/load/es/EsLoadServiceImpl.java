package com.jte.sync2es.load.es;

import com.jte.sync2es.load.LoadService;
import com.jte.sync2es.model.es.EsRequest;
import com.jte.sync2es.model.mysql.TableMeta;
import com.jte.sync2es.model.mysql.TableRecords;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;

import javax.annotation.Resource;
import java.io.IOException;

public class EsLoadServiceImpl implements LoadService {

    @Resource
    RestHighLevelClient client;

    @Override
    public int operateData(EsRequest request) throws IOException {
        checkAndCreateStorage(request);
        return 0;
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
            //TODO
        }
        else
        {
            LOAD_STORAGE.add("es$"+tableMeta.getDbName()+"$"+tableMeta.getTableName());
        }
    }


}

package com.jte.sync2any.load.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.extract.KafkaMsgListener;
import com.jte.sync2any.load.LoadService;
import com.jte.sync2any.model.es.EsDateType;
import com.jte.sync2any.model.es.EsRequest;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
public class EsLoadServiceImpl implements LoadService {

    @Resource
    RestHighLevelClient client;
    private final Map<String,Long> cacheCount=new HashMap<>();

    @Override
    public int operateData(EsRequest request) throws IOException {
        checkAndCreateStorage(request.getTableMeta());
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
        IndexRequest addIndex = new IndexRequest(request.getIndex());
        addIndex.id(request.getDocId());
        addIndex.source(JsonUtil.objectToJson(request.getParameters()), XContentType.JSON);
        IndexResponse indexResponse = client.index(addIndex, RequestOptions.DEFAULT);
        if(indexResponse.status().equals(RestStatus.OK)&&indexResponse.getShardInfo().getSuccessful()>0)
        {
            return 1;
        }
        return 0;
    }

    @Override
    public int deleteData(EsRequest request) throws IOException {
        DeleteRequest deleteIndex = new DeleteRequest(request.getIndex(),request.getDocId());
        DeleteResponse deleteResponse = client.delete(deleteIndex, RequestOptions.DEFAULT);
        if(deleteResponse.status().equals(RestStatus.OK)&&deleteResponse.getShardInfo().getSuccessful()>0)
        {
            return 1;
        }
        return 0;
    }

    @Override
    public int updateData(EsRequest request) throws IOException {
        UpdateRequest updateIndex = new UpdateRequest(request.getIndex(),request.getDocId());
        updateIndex.doc(JsonUtil.objectToJson(request.getParameters()), XContentType.JSON);
        updateIndex.docAsUpsert(true);
        UpdateResponse updateResponse = client.update(updateIndex, RequestOptions.DEFAULT);
        if(updateResponse.status().equals(RestStatus.OK)&&updateResponse.getShardInfo().getSuccessful()>0)
        {
            return 1;
        }
        return 0;
    }

    @Override
    public int batchAdd(List<EsRequest> requestList) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        requestList.forEach(r->{
            IndexRequest addIndex = new IndexRequest(r.getIndex());
            addIndex.id(r.getDocId());
            addIndex.source(JsonUtil.objectToJson(r.getParameters()), XContentType.JSON);
            bulkRequest.add(addIndex);
        });
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
            return 0;
        }
        return requestList.size();
    }

    /**
     * 此函数谨慎调用，需看懂它的意思
     * ！！！！注意！！！！
     * 此接口对于每个esIndex在整个应用的生命周期只会查一次，之后的对此函数的调用都会返回第一次查询的值。
     * 这么做的原因是，为了多个分表（rule使用正则表达式）同步到同一个esIndex时，第一次dump数据的完整性。不然只会同步第一个分表的数据。
     * ！！！！注意！！！！
     * 查看es的index是否存在且有数据。
     * @param esIndex
     * @return
     * @throws IOException
     */
    @Override
    public Long countData(String esIndex) throws IOException {
        Long count=cacheCount.get(esIndex);
        if(Objects.nonNull(count))
        {
            return count;
        }
        if(!isIndexExists(esIndex))
        {
            cacheCount.put(esIndex,0L);
            return 0L;
        }
        CountRequest countRequest = new CountRequest(esIndex);
        countRequest.query(QueryBuilders.matchAllQuery());
        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
        count= countResponse.getCount();
        cacheCount.put(esIndex,count);
        return count;
    }

    /**
     * es index命名规则：dbName-tableName
     * @param tableMeta
     * @return
     */
    @Override
    public void checkAndCreateStorage(TableMeta tableMeta) throws IOException {
        String indexName=tableMeta.getEsIndexName();
        String existsEvidence="es$"+tableMeta.getDbName()+"$"+tableMeta.getTableName();
        if(LOAD_STORAGE.contains(existsEvidence)){
            return;
        }
        boolean indexExists= isIndexExists(indexName);
        if(!indexExists)
        {
            String mappingJson=generateMappingJson(tableMeta);
            //不存在，创建映射关系
            CreateIndexRequest newMapping = new CreateIndexRequest(indexName);
            newMapping.settings(Settings.builder()
                    .put("index.number_of_shards", 8)
                    .put("index.number_of_replicas", 2)
            );
            newMapping.mapping(mappingJson, XContentType.JSON);
            log.info("add new index mapping for {}",indexName);
            CreateIndexResponse createIndexResponse = client.indices().create(newMapping, RequestOptions.DEFAULT);
            if(!createIndexResponse.isAcknowledged())
            {
                throw new ShouldNeverHappenException("create mapping failed!");
            }
            LOAD_STORAGE.add(existsEvidence);
        }
        else
        {
            LOAD_STORAGE.add(existsEvidence);
        }
    }

    public boolean isIndexExists(String indexName) throws IOException {
        GetIndexRequest indexRequest = new GetIndexRequest(indexName);
        return client.indices().exists(indexRequest,RequestOptions.DEFAULT);
    }

    /**
     * 根据我放从数据库取得的表结构以及用户自己的配置，来创建一个描述index的mapping的字符串
     * 结构大概如下
     *   {
     *     "properties": {
     *       "age":    { "type": "integer" },
     *       "email":  {
     *         "type": "text","fields": {
     *           "raw": {
     *             "type":  "keyword"
     *           }
     *         }
     *       },
     *       "name":   { "type": "text"  }
     *     }
     *   }
     * @param tableMeta
     */
    public String generateMappingJson(TableMeta tableMeta)
    {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode properties = mapper.createObjectNode();
        ObjectNode property = mapper.createObjectNode();
        properties.set("properties",property);
        tableMeta.getAllColumnList().forEach(c->{
            ObjectNode typeValue=null;
            if(c.getEsDataType().equals(EsDateType.TEXT.getDataType())||c.getEsDataType().equals(EsDateType.KEYWORD.getDataType()))
            {
                ObjectNode rowValue=mapper.createObjectNode();
                rowValue.put("type","text");
                rowValue.put("analyzer","ik_max_word");
                rowValue.put("search_analyzer","ik_smart");
                ObjectNode rowFieldsValue=mapper.createObjectNode().set("ser",rowValue);
                typeValue=mapper.createObjectNode()
                        .put("type","keyword")
                        .set("fields",rowFieldsValue);
            }
            else if(c.getEsDataType().equals(EsDateType.DATA.getDataType()))
            {
                typeValue=mapper.createObjectNode()
                        .put("type",c.getEsDataType())
                        .put("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy:MM:dd||epoch_millis");
            }
            else
            {
                typeValue=mapper.createObjectNode().put("type",c.getEsDataType());
            }
            property.set(c.getEsColumnName(),typeValue);
        });
        return properties.toString();
    }


}

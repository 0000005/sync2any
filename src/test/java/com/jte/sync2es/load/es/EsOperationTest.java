package com.jte.sync2es.load.es;

import com.jte.sync2es.Tester;
import com.jte.sync2es.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.junit.Test;

import javax.annotation.Resource;
import java.io.IOException;

@Slf4j
public class EsOperationTest extends Tester {

    @Resource
    RestHighLevelClient client;

    @Test
    public void getMappingTest() throws IOException {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices("kibana_sample_data_ecommerce");
        GetMappingsResponse response=client.indices().getMapping(request, RequestOptions.DEFAULT);
        System.out.println(JsonUtil.objectToJson(response.mappings()));
    }

    @Test
    public void getIndexInfo() throws IOException {
        GetIndexRequest request = new GetIndexRequest("kibana_sample_data_ecommerce");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }


}

package com.jte.sync2es.conf;

import com.jte.sync2es.model.config.Elasticsearch;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class ElasticsearchConfig {

    @Autowired
    Elasticsearch elasticsearch;

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        if(StringUtils.isBlank(elasticsearch.getUris()))
        {
            log.error("请填写elasticsearch的相关配置。");
            System.exit(500);
        }
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticsearch.getUsername(), elasticsearch.getPassword()));

        String [] urlArray=elasticsearch.getUris().split(",");
        HttpHost [] hostArray = new HttpHost[urlArray.length];
        for(int i =0;i<urlArray.length;i++)
        {
            String [] host=urlArray[i].split(":");
            hostArray[i]=new HttpHost(host[0],Integer.valueOf(host[1]));
        }
        RestClientBuilder builder=RestClient.builder(hostArray);
        builder.setHttpClientConfigCallback(httpClientBuilder ->
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }


}

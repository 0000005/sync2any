package com.jte.sync2any.util;

import com.jte.sync2any.core.Constants;
import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.model.config.Conn;
import com.jte.sync2any.model.mysql.MyDatasource;
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
import org.springframework.jdbc.core.JdbcTemplate;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class DbUtils {
    /**
     * 去处字符串首尾的单引号
     * @param origin
     * @return
     */
    public static String delQuote(String origin)
    {
        if(StringUtils.isBlank(origin))
        {
            return origin;
        }
        if(origin.indexOf("'")==0&&origin.lastIndexOf("'")==(origin.length()-1))
        {
            return origin.substring(1,origin.length()-1);
        }
        return origin;
    }

    /**
     * 从数据库配置信息中解析出 host、端口等信息
     * @param url
     * @return
     */
    public static Map<String,String> getParamFromUrl(String url)
    {
        Map<String,String> param= new HashMap<>();
        String cleanURI = url.substring(5);
        URI uri = URI.create(cleanURI);
        param.put("type",uri.getScheme());
        param.put("host",uri.getHost());
        param.put("port",new Integer(uri.getPort()).toString());
        param.put("param",uri.getPath());
        return param;
    }


    public static RestHighLevelClient getEsDatasource(Conn conn){

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(conn.getUsername(), conn.getPassword()));

        String [] urlArray=conn.getUrl().split(",");
        HttpHost[] hostArray = new HttpHost[urlArray.length];
        for(int i =0;i<urlArray.length;i++)
        {
            String [] host=urlArray[i].split(":");
            hostArray[i]=new HttpHost(host[0],Integer.valueOf(host[1]));
        }
        RestClientBuilder builder= RestClient.builder(hostArray);
        builder.setHttpClientConfigCallback(httpClientBuilder ->
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static JdbcTemplate getMysqlDatasource(Conn conn){
        MyDatasource mysqlDs = new MyDatasource();
        mysqlDs.setDriverClassName(Constants.JDBC_CLASS_NAME);
        mysqlDs.setUrl(conn.getUrl());
        mysqlDs.setUsername(conn.getUsername());
        mysqlDs.setPassword(conn.getPassword());
        try
        {
            mysqlDs.setDbName(mysqlDs.getConnection().getCatalog());
            conn.setDbName(mysqlDs.getDbName());
        }
        catch (Exception e)
        {
            log.error("获取数据库名失败",e);
        }
        return new JdbcTemplate(mysqlDs);
    }


    public static Conn getConnByDbId(List<Conn> dsList, String dbId){
        return dsList.stream().filter(e->e.getDbId().equals(dbId)).findFirst().orElse(null);
    }


    public static Object getTargetDsByDbId(Map<String, Object> allTargetDatasource, String dbId){
        Object ds=allTargetDatasource.get(dbId);
        if(Objects.isNull(ds)){
            throw new ShouldNeverHappenException("target datasource id not found, dbId:"+dbId);
        }
        return ds;
    }
}

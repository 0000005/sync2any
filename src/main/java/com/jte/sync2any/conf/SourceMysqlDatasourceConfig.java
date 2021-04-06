package com.jte.sync2any.conf;

import com.jte.sync2any.core.Constants;
import com.jte.sync2any.model.config.Conn;
import com.jte.sync2any.model.config.SourceMysqlDb;
import com.jte.sync2any.model.mysql.MyDatasource;
import com.jte.sync2any.model.mysql.OtherDataSources;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 源数据库的初始化
 */
@Configuration
@Slf4j
public class SourceMysqlDatasourceConfig {
    @Resource
    SourceMysqlDb sourceMysqlDb;

    @Bean("primaryDataSource")
    @Primary
    public DataSource primaryDataSource() {
        if(sourceMysqlDb.getDatasources().size()==0)
        {
            log.error("请填写请至少填写一个mysql配置。");
            System.exit(500);
        }
        Conn conn= sourceMysqlDb.getDatasources().get(0);
        MyDatasource dataSource = new MyDatasource();
        dataSource.setDbName(conn.getDbName());
        dataSource.setDriverClassName(Constants.JDBC_CLASS_NAME);
        dataSource.setUrl(conn.getUrl());
        dataSource.setUsername(conn.getUsername());
        dataSource.setPassword(conn.getPassword());
        return dataSource;
    }

    @Bean("otherDataSource")
    public OtherDataSources otherDataSource() {
        OtherDataSources ods = new OtherDataSources();
        List<Conn> connList= sourceMysqlDb.getDatasources();
        if(connList.size()<=1)
        {
            return ods;
        }
        for(int i =1;i<connList.size();i++)
        {
            Conn conn=connList.get(i);
            MyDatasource dataSource = new MyDatasource();
            dataSource.setDbName(conn.getDbName());
            dataSource.setDriverClassName(Constants.JDBC_CLASS_NAME);
            dataSource.setUrl(conn.getUrl());
            dataSource.setUsername(conn.getUsername());
            dataSource.setPassword(conn.getPassword());
            ods.getDsList().add(dataSource);
        }

        return ods;
    }

    @Bean("allSourceTemplate")
    public Map<String,JdbcTemplate> allSourceTemplate(@Qualifier("primaryDataSource") DataSource ds,
                                                @Qualifier("otherDataSource") OtherDataSources ods)
    {
        //primary
        Map<String,JdbcTemplate> allSourceTemplate=new HashMap<>();
        allSourceTemplate.put(((MyDatasource)ds).getDbName(),new JdbcTemplate(ds));
        //other
        List<DataSource> otherDsList=ods.getDsList();
        if (!otherDsList.isEmpty()) {
            for(DataSource myDs:otherDsList)
            {
                String dbName=((MyDatasource)myDs).getDbName();
                if(allSourceTemplate.containsKey(dbName))
                {
                    log.error("在mysql数据源的配置中，不允许出现重复的数据库名称！");
                    System.exit(500);
                }
                allSourceTemplate.put(dbName,new JdbcTemplate(ds));
            }
        }
        return allSourceTemplate;
    }


}

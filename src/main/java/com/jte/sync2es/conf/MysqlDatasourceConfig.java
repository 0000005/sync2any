package com.jte.sync2es.conf;

import com.jte.sync2es.model.config.Conn;
import com.jte.sync2es.model.config.MysqlDb;
import com.jte.sync2es.model.mysql.MyDatasource;
import com.jte.sync2es.model.mysql.OtherDataSources;
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

@Configuration
@Slf4j
public class MysqlDatasourceConfig {
    @Resource
    MysqlDb mysqlDb;

    @Bean("primaryDataSource")
    @Primary
    public DataSource primaryDataSource() {
        if(mysqlDb.getDatasources().size()==0)
        {
            log.error("请填写请至少填写一个mysql配置。");
            System.exit(500);
        }
        Conn conn=mysqlDb.getDatasources().get(0);
        MyDatasource dataSource = new MyDatasource();
        dataSource.setDbName(conn.getDbName());
        dataSource.setDriverClassName(conn.getDriverClassName());
        dataSource.setUrl(conn.getUrl());
        dataSource.setUsername(conn.getUsername());
        dataSource.setPassword(conn.getPassword());
        return dataSource;
    }

    @Bean("otherDataSource")
    public OtherDataSources otherDataSource() {
        OtherDataSources ods = new OtherDataSources();
        List<Conn> connList=mysqlDb.getDatasources();
        if(connList.size()<=1)
        {
            return ods;
        }
        for(int i =1;i<connList.size();i++)
        {
            Conn conn=connList.get(i);
            MyDatasource dataSource = new MyDatasource();
            dataSource.setDbName(conn.getDbName());
            dataSource.setDriverClassName(conn.getDriverClassName());
            dataSource.setUrl(conn.getUrl());
            dataSource.setUsername(conn.getUsername());
            dataSource.setPassword(conn.getPassword());
            ods.getDsList().add(dataSource);
        }

        return ods;
    }

    @Bean("allTemplate")
    public Map<String,JdbcTemplate> allTemplate(@Qualifier("primaryDataSource") DataSource ds,
                                                @Qualifier("otherDataSource") OtherDataSources ods)
    {
        //primary
        Map<String,JdbcTemplate> allTemplate=new HashMap<>();
        allTemplate.put(((MyDatasource)ds).getDbName(),new JdbcTemplate(ds));
        //other
        List<DataSource> otherDsList=ods.getDsList();
        if (!otherDsList.isEmpty()) {
            for(DataSource myDs:otherDsList)
            {
                allTemplate.put(((MyDatasource)myDs).getDbName(),new JdbcTemplate(ds));
            }
        }
        return allTemplate;
    }


}

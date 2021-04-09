package com.jte.sync2any.conf;

import com.jte.sync2any.model.config.Conn;
import com.jte.sync2any.model.config.SourceMysqlDb;
import com.jte.sync2any.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Resource;
import java.util.*;

/**
 * 源数据库的初始化
 */
@Configuration
@Slf4j
public class SourceMysqlDatasourceConfig {
    @Resource
    SourceMysqlDb sourceMysqlDb;

    @Bean("allSourceTemplate")
    public Map<String,JdbcTemplate> allSourceTemplate()
    {
        if(sourceMysqlDb.getDatasources().size()==0)
        {
            log.error("请填写请至少填写一个mysql配置。");
            System.exit(500);
        }
        Set<String> dbIdSet = new HashSet<>();
        Map<String,JdbcTemplate> allSourceTemplate=new HashMap<>();
        List<Conn> connList= sourceMysqlDb.getDatasources();
        for(int i =1;i<connList.size();i++)
        {
            Conn conn=connList.get(i);
            if(StringUtils.isBlank(conn.getUrl())||StringUtils.isBlank(conn.getDbName())||StringUtils.isBlank(conn.getUsername()))
            {
                log.error("请正确填写源数据库的相关配置。");
                System.exit(500);
            }
            if(StringUtils.isBlank(conn.getDbId()))
            {
                log.error("请填写db-id！");
                System.exit(500);
            }
            if(dbIdSet.contains(conn.getDbId()))
            {
                log.error("发现重复的db-id！");
                System.exit(500);
            }
            if(allSourceTemplate.containsKey(conn.getUrl()))
            {
                log.error("在mysql数据源的配置中，不允许出现重复的数据库名称！");
                System.exit(500);
            }
            JdbcTemplate jdbcTemplate=DbUtils.getMysqlDatasource(conn);
            allSourceTemplate.put(conn.getDbId(),jdbcTemplate);
            dbIdSet.add(conn.getDbId());
        }

        return allSourceTemplate;
    }

}

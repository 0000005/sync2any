package com.jte.sync2any.conf;

import com.jte.sync2any.model.config.TargetDatasources;
import com.jte.sync2any.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.jte.sync2any.model.config.Conn.*;

/**
 * 初始化同步目标数据源
 */
@Configuration
@Slf4j
public class TargetDatasourcesConfig {


    @Resource
    private TargetDatasources targetDatasources;

    @Bean("allTargetDatasource")
    public Map<String,Object> allTargetDatasource()
    {
        Map<String,Object> targetDsMap = new HashMap<>();
        Set<String> dbIdSet = new HashSet<>();
        targetDatasources.getDatasources().stream().forEach(conn->{
            if(StringUtils.isBlank(conn.getUrl())||StringUtils.isBlank(conn.getUsername()))
            {
                log.error("请填写elasticsearch的相关配置。");
                System.exit(500);
            }
            if(StringUtils.isBlank(conn.getDbId()))
            {
                log.error("请填写db-id");
                System.exit(500);
            }
            if(dbIdSet.contains(conn.getDbId()))
            {
                log.error("发现重复的db-id！");
                System.exit(500);
            }
            if(DB_TYPE_ES.equals(conn.getType()))
            {
                targetDsMap.put(conn.getDbId(), DbUtils.getEsDatasource(conn));
            }
            else if(DB_TYPE_MYSQL.equals(conn.getType()))
            {
                targetDsMap.put(conn.getDbId(),DbUtils.getMysqlDatasource(conn));
            }
            else if(DB_TYPE_CLICKHOUSE.equals(conn.getType()))
            {
                targetDsMap.put(conn.getDbId(),DbUtils.getCkDatasource(conn));
            }
            else
            {
                log.error("目标数据源未知的type。");
                System.exit(500);
            }
            dbIdSet.add(conn.getDbId());
        });
        return targetDsMap;
    }


}

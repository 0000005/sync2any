package com.jte.sync2es.conf;

import com.jte.sync2es.MonitorTask;
import com.jte.sync2es.model.config.Elasticsearch;
import com.jte.sync2es.model.config.Sync2es;
import com.wangfengta.api.WftClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import javax.annotation.Resource;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.jte.sync2es.conf.RuleConfigParser.RULES_MAP;

@Configuration
@Slf4j
public class AlarmConfig {

    @Autowired
    Elasticsearch elasticsearch;
    @Resource
    Sync2es sync2es;

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    @Bean
    public WftClient wftClient()
    {
        if(StringUtils.isBlank(sync2es.getAlert().getSecret()))
        {
            log.warn("！！！！！！！！建议使用“望风塔”（wangfengta.com）进行监控告警！！！！！！！！！");
            return null;
        }
        return WftClient.init(sync2es.getAlert().getSecret());
    }


    @EventListener(ApplicationReadyEvent.class)
    public void startMonitor(){
        WftClient wftClient=wftClient();
        if(Objects.nonNull(wftClient))
        {
            executor.scheduleWithFixedDelay(new MonitorTask(RULES_MAP.asMap(),sync2es,wftClient), 5, 5, TimeUnit.SECONDS);
        }
    }
}

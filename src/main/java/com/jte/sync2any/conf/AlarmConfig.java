package com.jte.sync2any.conf;

import com.jte.sync2any.MonitorTask;
import com.jte.sync2any.model.config.Alert;
import com.jte.sync2any.model.config.Sync2any;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.jte.sync2any.MonitorTask.LOOP_WAITING_TIME;
import static com.jte.sync2any.conf.RuleConfigParser.RULES_MAP;

@Configuration
@Slf4j
public class AlarmConfig {

    @Resource
    Sync2any sync2any;

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);


    public void startMonitor() {
        log.warn("！！！！！！！！start monitor ！！！！！！！！！");
        Alert alertConf = sync2any.getAlert();
        if (Objects.isNull(alertConf) || StringUtils.isBlank(alertConf.getTouid())) {
            log.warn("！！！！！！！！建议使用“SERVER酱”进行监控告警！！！！！！！！！");
        } else {
            executor.scheduleWithFixedDelay(new MonitorTask(RULES_MAP.asMap(), sync2any), 5, LOOP_WAITING_TIME, TimeUnit.SECONDS);
        }
    }
}

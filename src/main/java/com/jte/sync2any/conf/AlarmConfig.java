package com.jte.sync2any.conf;

import com.jte.sync2any.MonitorTask;
import com.jte.sync2any.model.config.Alert;
import com.jte.sync2any.model.config.Sync2any;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import javax.annotation.Resource;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.jte.sync2any.conf.RuleConfigParser.RULES_MAP;

@Configuration
@Slf4j
public class AlarmConfig {

    @Resource
    Sync2any sync2any;

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);


    /**
     * 项目启动完毕开始触发运行定时扫描监控任务
     */
    @EventListener(ApplicationReadyEvent.class)
    public void startMonitor() {
        Alert alertConf = sync2any.getAlert();
        if (Objects.isNull(alertConf) || StringUtils.isBlank(alertConf.getSecret())) {
            log.warn("！！！！！！！！建议使用“SERVER酱”进行监控告警！！！！！！！！！");
        } else {
            executor.scheduleWithFixedDelay(new MonitorTask(RULES_MAP.asMap(), sync2any), 5, 5, TimeUnit.SECONDS);
        }
    }
}

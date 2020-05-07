package com.jte.sync2es;

import com.jte.sync2es.model.config.Sync2es;
import com.jte.sync2es.model.config.SyncConfig;
import com.jte.sync2es.model.core.SyncState;
import com.jte.sync2es.model.mysql.TableMeta;
import com.wangfengta.api.WftClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class MonitorTask implements  Runnable {

    private Map<String, TableMeta> tableRules;
    private Sync2es sync2es;
    private WftClient wftClient;

    public MonitorTask(Map<String, TableMeta> tableRules, Sync2es sync2es, WftClient wftClient) {
        this.tableRules=tableRules;
        this.sync2es=sync2es;
        this.wftClient=wftClient;
    }

    @Override
    public void run() {
        log.debug("run monitor...");
        for(SyncConfig config:sync2es.getSyncConfigList())
        {
            int maxDelayInSecond=config.getMaxDelayInSecond();
            int maxIdleInMinute=config.getMaxIdleInMinute();
            int nextTriggerAlertInMinute=config.getNextTriggerAlertInMinute();

            //匹配对应的TableMeta(可能包含正则)
            List<String> syncTableList = Arrays.asList(config.getSyncTables().split(","));
            //先过滤数据库
            Set<String> metaKeySet=tableRules.keySet()
                    .stream()
                    .filter(k->k.startsWith(config.getDbName()+"$"))
                    .collect(Collectors.toSet());
            //再过滤表
            for(String metaKey:metaKeySet)
            {
                String metaTableName=metaKey.split("\\$")[1];
                boolean isMatch=syncTableList
                        .stream()
                        .filter(t->Pattern.matches(t, metaTableName))
                        .count()>0;
                if(isMatch)
                {
                    TableMeta currTableMeta=tableRules.get(metaKey);

                    if(currTableMeta.getState().equals(SyncState.WAITING)||currTableMeta.getState().equals(SyncState.LOADING_ORIGIN_DATA))
                    {
                        continue;
                    }

                    long alertGapTime=System.currentTimeMillis()-currTableMeta.getLastAlarmTime();
                    long alertGapTimeInMinute=alertGapTime/1000/60;
                    if(alertGapTimeInMinute<nextTriggerAlertInMinute)
                    {
                        continue;
                    }

                    if(currTableMeta.getState().equals(SyncState.STOPPED)&&StringUtils.isNotBlank(sync2es.getAlert().getErrorTemplateId()))
                    {
                        try
                        {
                            log.error("alarm stopped");
//                            wftClient.alarm(sync2es.getAlert().getAppId(),sync2es.getAlert().getErrorTemplateId());
                            currTableMeta.setLastAlarmTime(System.currentTimeMillis());
                            continue;
                        }
                        catch (Exception e)
                        {
                            log.error("stopped alarm fail,",e);
                        }
                    }

                    long delay=currTableMeta.getLastSyncTime()-currTableMeta.getLastDataManipulateTime();
                    if(maxDelayInSecond!=-1 && (delay/1000)>maxDelayInSecond&&StringUtils.isNotBlank(sync2es.getAlert().getDelayTemplateId()))
                    {
                        try
                        {
                            log.error("alarm delay");
//                            wftClient.alarm(sync2es.getAlert().getAppId(),sync2es.getAlert().getDelayTemplateId(),null);
                            currTableMeta.setLastAlarmTime(System.currentTimeMillis());
                        }
                        catch (Exception e)
                        {
                            log.error("delay alarm fail,",e);
                        }
                    }
                    long idle=currTableMeta.getLastSyncTime();
                    if(idle!=0)
                    {
                        idle=System.currentTimeMillis()-currTableMeta.getLastSyncTime();
                    }
                    long idleTimeInMinute=idle/1000/60;
                    if(maxIdleInMinute!=-1&& idleTimeInMinute>maxIdleInMinute&&StringUtils.isNotBlank(sync2es.getAlert().getIdleTemplateId()))
                    {
                        try
                        {
                            log.error("alarm idle");
//                            wftClient.alarm(sync2es.getAlert().getAppId(),sync2es.getAlert().getIdleTemplateId());
                            currTableMeta.setLastAlarmTime(System.currentTimeMillis());
                        }
                        catch (Exception e)
                        {
                            log.error("idle alarm fail,",e);
                        }
                    }

                }
            }
        }
    }
}

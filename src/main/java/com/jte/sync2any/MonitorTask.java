package com.jte.sync2any;

import com.jte.sync2any.model.config.SyncConfig;
import com.jte.sync2any.model.config.sync2any;
import com.jte.sync2any.model.core.SyncState;
import com.jte.sync2any.model.mysql.TableMeta;
import com.wangfengta.api.WftClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class MonitorTask implements  Runnable {

    private Map<String, TableMeta> tableRules;
    private sync2any sync2any;
    private WftClient wftClient;

    public MonitorTask(Map<String, TableMeta> tableRules, sync2any sync2any, WftClient wftClient) {
        this.tableRules=tableRules;
        this.sync2any=sync2any;
        this.wftClient=wftClient;
    }

    @Override
    public void run() {
        log.debug("run monitor...");
        for(SyncConfig config:sync2any.getSyncConfigList())
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

                    if(currTableMeta.getState().equals(SyncState.STOPPED)&&StringUtils.isNotBlank(sync2any.getAlert().getErrorTemplateId()))
                    {
                        try
                        {
                            Map<String,String> param=assmbleAlertParam(currTableMeta,"error",currTableMeta.getErrorReason());
                            wftClient.alarm(sync2any.getAlert().getAppId(),sync2any.getAlert().getErrorTemplateId(),param);
                            currTableMeta.setLastAlarmTime(System.currentTimeMillis());
                            continue;
                        }
                        catch (Exception e)
                        {
                            log.error("stopped alarm fail,",e);
                        }
                    }

                    long delay=currTableMeta.getLastSyncTime()-currTableMeta.getLastDataManipulateTime();
                    if(maxDelayInSecond!=-1 && (delay/1000)>maxDelayInSecond&&StringUtils.isNotBlank(sync2any.getAlert().getDelayTemplateId()))
                    {
                        try
                        {
                            Map<String,String> param=assmbleAlertParam(currTableMeta,"delay",String.valueOf((delay/1000)));
                            wftClient.alarm(sync2any.getAlert().getAppId(),sync2any.getAlert().getDelayTemplateId(),param);
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
                    if(maxIdleInMinute!=-1&& idleTimeInMinute>maxIdleInMinute&&StringUtils.isNotBlank(sync2any.getAlert().getIdleTemplateId()))
                    {
                        try
                        {
                            Map<String,String> param=assmbleAlertParam(currTableMeta,"idleTime",String.valueOf(idleTimeInMinute));
                            wftClient.alarm(sync2any.getAlert().getAppId(),sync2any.getAlert().getIdleTemplateId(),param);
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

    private Map<String,String> assmbleAlertParam(TableMeta meta,String key,String value)
    {
        Map<String,String> param = new HashMap<>(10);
        param.put("tables",meta.getTableName());
        param.put("dbName",meta.getDbName());
        param.put("indexName",meta.getEsIndexName());
        param.put(key,value);
        return param;
    }
}

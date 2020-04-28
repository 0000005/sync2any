package com.jte.sync2es;

import com.jte.sync2es.model.config.Sync2es;
import com.jte.sync2es.model.config.SyncConfig;
import com.jte.sync2es.model.mysql.TableMeta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MonitorTask implements  Runnable {

    private Map<String, TableMeta> tableRules;
    private Sync2es sync2es;
    public MonitorTask(Map<String, TableMeta> tableRules,Sync2es sync2es) {
        this.tableRules=tableRules;
        this.sync2es=sync2es;
    }

    @Override
    public void run() {
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
                    if(maxDelayInSecond!=-1)
                    {

                    }
                    if(maxIdleInMinute!=-1)
                    {

                    }
                }
            }


        }

    }
}

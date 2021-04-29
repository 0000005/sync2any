package com.jte.sync2any;

import com.jte.sync2any.model.config.Sync2any;
import com.jte.sync2any.model.config.SyncConfig;
import com.jte.sync2any.model.core.SyncState;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.AlertUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class MonitorTask implements Runnable {

    private Map<String, TableMeta> tableRules;
    private Sync2any sync2any;

    public MonitorTask(Map<String, TableMeta> tableRules, Sync2any sync2any) {
        this.tableRules = tableRules;
        this.sync2any = sync2any;
    }

    @Override
    public void run() {
        log.debug("run monitor...");
        for (SyncConfig config : sync2any.getSyncConfigList()) {
            int maxDelayInSecond = config.getMaxDelayInSecond();
            int maxIdleInMinute = config.getMaxIdleInMinute();
            int nextTriggerAlertInMinute = config.getNextTriggerAlertInMinute();

            //先过滤数据库
            Set<String> metaKeySet = tableRules.keySet()
                    .stream()
                    .filter(k -> k.startsWith(config.getSourceDbId() + "$"))
                    .collect(Collectors.toSet());
            //再过滤表
            for (String metaKey : metaKeySet) {

                TableMeta currTableMeta = tableRules.get(metaKey);
                if (currTableMeta.getState().equals(SyncState.WAITING) || currTableMeta.getState().equals(SyncState.LOADING_ORIGIN_DATA)) {
                    continue;
                }

                long alertGapTime = System.currentTimeMillis() - currTableMeta.getLastAlarmTime();
                long alertGapTimeInMinute = alertGapTime / 1000 / 60;
                if (alertGapTimeInMinute < nextTriggerAlertInMinute) {
                    continue;
                }

                //意外停止
                if (currTableMeta.getState().equals(SyncState.STOPPED)) {
                    try {
                        String msg = assembleAlertParam(currTableMeta, "意外停止同步", currTableMeta.getErrorReason());
                        AlertUtils.sendAlert(sync2any.getAlert().getSecret(),msg);
                        currTableMeta.setLastAlarmTime(System.currentTimeMillis());
                        continue;
                    } catch (Exception e) {
                        log.error("stopped alarm fail,", e);
                    }
                }

                //同步延迟
                long delay = currTableMeta.getLastSyncTime() - currTableMeta.getLastDataManipulateTime();
                if (maxDelayInSecond != -1 && (delay / 1000) > maxDelayInSecond) {
                    try {
                        String msg = assembleAlertParam(currTableMeta, "延迟时间超过阈值", String.valueOf((delay / 1000)));
                        AlertUtils.sendAlert(sync2any.getAlert().getSecret(),msg);
                        currTableMeta.setLastAlarmTime(System.currentTimeMillis());
                    } catch (Exception e) {
                        log.error("delay alarm fail,", e);
                    }
                }

                //空闲时间
                long idle = currTableMeta.getLastSyncTime();
                if (idle != 0) {
                    idle = System.currentTimeMillis() - currTableMeta.getLastSyncTime();
                }
                long idleTimeInMinute = idle / 1000 / 60;
                if (maxIdleInMinute != -1 && idleTimeInMinute > maxIdleInMinute) {
                    try {
                        String msg = assembleAlertParam(currTableMeta, "空闲时间超过阈值，", String.valueOf(idleTimeInMinute*60));
                        AlertUtils.sendAlert(sync2any.getAlert().getSecret(),msg);
                        currTableMeta.setLastAlarmTime(System.currentTimeMillis());
                    } catch (Exception e) {
                        log.error("idle alarm fail,", e);
                    }
                }
            }
        }
    }

    private String assembleAlertParam(TableMeta meta, String key, String value) {
        StringBuffer sb = new StringBuffer();
        sb.append("sync2any数据同步异常，");
        sb.append("异常原因：");
        sb.append(key);
        sb.append(" 异常：");
        sb.append(value);
        sb.append("秒，源数据库：，");
        sb.append(meta.getDbName());
        sb.append("源表名：，");
        sb.append(meta.getTableName());
        sb.append("同步目标：，");
        sb.append(meta.getTargetTableName());
        return sb.toString();
    }
}

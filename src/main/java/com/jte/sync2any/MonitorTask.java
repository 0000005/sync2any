package com.jte.sync2any;

import com.jte.sync2any.model.config.Sync2any;
import com.jte.sync2any.model.config.SyncConfig;
import com.jte.sync2any.model.core.SyncState;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.AlertUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class MonitorTask implements Runnable {

    private Map<String, TableMeta> tableRules;
    private Sync2any sync2any;
    /**
     * 同步消息时发生错误的Topic。
     */
    public static List<String> ERROR_TOPIC_LIST = new ArrayList<>();
    /**
     * 同步消息时发送的错误，2个小时检测一次
     */
    private final int errorTopicAlertTimeGap = 60 * 60 * 2;
    private int currentErrorTopicAlertTimeGap = 0;

    /**
     * 阁多少秒循环检测一次
     */
    public static int LOOP_WAITING_TIME = 6;

    public MonitorTask(Map<String, TableMeta> tableRules, Sync2any sync2any) {
        this.tableRules = tableRules;
        this.sync2any = sync2any;
    }

    @Override
    public void run() {
        log.debug("run monitor...");
        //检测kafka同步过程中是否出错
        //计时累计
        currentErrorTopicAlertTimeGap = currentErrorTopicAlertTimeGap + LOOP_WAITING_TIME;
        if (currentErrorTopicAlertTimeGap > errorTopicAlertTimeGap) {
            if (ERROR_TOPIC_LIST.isEmpty()) {
                AlertUtils.sendAlert(sync2any.getAlert().getSecret(), "发现有消息同步失败:" + ERROR_TOPIC_LIST.toString());
                ERROR_TOPIC_LIST.clear();
            }
            //置空重新计时
            currentErrorTopicAlertTimeGap = 0;
        }

        //检测每个表的同步状况
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
                    String msg = assembleAlertParam(currTableMeta, "意外停止同步", currTableMeta.getErrorReason());
                    AlertUtils.sendAlert(sync2any.getAlert().getSecret(), msg);
                    currTableMeta.setLastAlarmTime(System.currentTimeMillis());
                    continue;
                }

                //同步延迟
                long delay = currTableMeta.getLastSyncTime() - currTableMeta.getLastDataManipulateTime();
                if (maxDelayInSecond != -1 && (delay / 1000) > maxDelayInSecond) {
                    String msg = assembleAlertParam(currTableMeta, "延迟时间超过阈值", String.valueOf((delay / 1000)));
                    AlertUtils.sendAlert(sync2any.getAlert().getSecret(), msg);
                    currTableMeta.setLastAlarmTime(System.currentTimeMillis());
                    continue;
                }

                //空闲时间
                long idle = currTableMeta.getLastSyncTime();
                if (idle != 0) {
                    idle = System.currentTimeMillis() - currTableMeta.getLastSyncTime();
                }
                long idleTimeInMinute = idle / 1000 / 60;
                if (maxIdleInMinute != -1 && idleTimeInMinute > maxIdleInMinute) {
                    String msg = assembleAlertParam(currTableMeta, "空闲时间超过阈值，", String.valueOf(idleTimeInMinute * 60));
                    AlertUtils.sendAlert(sync2any.getAlert().getSecret(), msg);
                    currTableMeta.setLastAlarmTime(System.currentTimeMillis());
                    continue;
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

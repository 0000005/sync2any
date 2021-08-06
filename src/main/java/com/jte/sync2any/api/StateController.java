package com.jte.sync2any.api;

import com.jte.sync2any.conf.KafkaConfig;
import com.jte.sync2any.conf.RuleConfigParser;
import com.jte.sync2any.extract.KafkaMsgListener;
import com.jte.sync2any.model.config.Mq;
import com.jte.sync2any.model.config.Sync2any;
import com.jte.sync2any.model.config.SyncConfig;
import com.jte.sync2any.model.core.SyncState;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.*;

import static com.jte.sync2any.conf.KafkaConfig.KAFKA_SET;

@Slf4j
@Controller
public class StateController {

    @Resource
    KafkaConfig kafkaConfig;
    @Resource
    Sync2any sync2any;

    /**
     * 获取所有任务的状态
     * 同步任务列表：
     * 每个同步任务包含：主题名称，主题组名称，数据库名，同步表名，同步字段名，状态【正在同步、异常停止同步】，同步延迟，最近同步时间，异常停止同步原因（如果发生的话）
     *
     * @return
     */
    @GetMapping("/")
    public String allState(ModelMap modelMap) {
        List<Map<String, String>> mapList = new ArrayList<>();
        Map<String, TableMeta> ruleMap = RuleConfigParser.RULES_MAP.asMap();
        int i = 1;
        for (String key : ruleMap.keySet()) {
            TableMeta meta = ruleMap.get(key);
            long delay = meta.getLastSyncTime() - meta.getLastDataManipulateTime();
            Map<String, String> row = new HashMap<>(10);
            row.put("index", i + "");
            row.put("dbName", meta.getDbName());
            row.put("tableName", meta.getTableName());
            row.put("esIndexName", meta.getTargetTableName());
            row.put("topicName", meta.getTopicName());
            row.put("topicGroup", meta.getTopicGroup());
            row.put("delay", delay / 1000 + "");
            row.put("state", meta.getState().desc());
            row.put("lastSyncTime", DateUtils.formatDate(new Date(meta.getLastSyncTime()), DateUtils.SHORT));
            row.put("tpq", meta.getTpq() + "");
            row.put("lastOffset", meta.getLastOffset() + "");
            row.put("errorReason", meta.getErrorReason() + "");
            i++;
            mapList.add(row);
        }
        modelMap.addAttribute("data", mapList);
        return "index";
    }

    /**
     * 重新设置kafka的fooset
     * @param topicName
     * @param topicGroup
     * @param sourceDbId 原始数据库id
     * @param offset
     * @return
     */
    @ResponseBody
    @PutMapping("offset")
    public String resetOffset(String topicName, String topicGroup , String sourceDbId, Long offset) {
        if(StringUtils.isBlank(topicName)||StringUtils.isBlank(sourceDbId)||StringUtils.isBlank(topicGroup)||offset==null){
            return "param error";
        }
        SyncConfig syncConfig = sync2any.findSyncConfigByTopicGroup(topicGroup);
        Mq mq = syncConfig.getMq();
        //停止当前的消费者
        KafkaMessageListenerContainer container = KafkaMsgListener.stopListener(topicName,topicGroup,sourceDbId, new RuntimeException("reset kafka offset"));
        KAFKA_SET.remove(container);

        //启动新的消费者
        container = kafkaConfig.createContainer(mq, offset, sourceDbId);
        if(KafkaConfig.canStartListener(container,mq.getTopicGroup(),mq.getTopicName())){
            log.info("kafka({}) start listening!",container.getBeanName());
            container.start();
            KAFKA_SET.add(container);
            //找到这个topicName 和 topicGroup对应的所有table,设置为同步中
            List<TableMeta> tableMetaList = RuleConfigParser.getTableMetaListByMq(topicName,topicGroup);
            tableMetaList.forEach(t->{
                t.setState(SyncState.SYNCING);
            });
            return  "ok";
        } else {
            log.info("failed to start listening!", container.getBeanName());
            return  "fail to start new listener";
        }
    }
}

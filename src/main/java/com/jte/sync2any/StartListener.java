package com.jte.sync2any;

import com.jte.sync2any.conf.AlarmConfig;
import com.jte.sync2any.conf.KafkaConfig;
import com.jte.sync2any.conf.RuleConfigParser;
import com.jte.sync2any.core.Constants;
import com.jte.sync2any.extract.OriginDataExtract;
import com.jte.sync2any.extract.impl.CkMetaExtractImpl;
import com.jte.sync2any.extract.impl.MysqlMetaExtractImpl;
import com.jte.sync2any.load.AbstractLoadService;
import com.jte.sync2any.model.config.Conn;
import com.jte.sync2any.model.config.TargetDatasources;
import com.jte.sync2any.model.core.SyncState;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.transform.DumpTransform;
import com.jte.sync2any.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

import javax.annotation.Resource;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.jte.sync2any.conf.RuleConfigParser.RULES_MAP;

/**
 * 在整个spring 容器启动之后，再进行业务上的启动
 */
@Configuration
@Slf4j
public class StartListener {

    @Resource
    MysqlMetaExtractImpl mysqlMetaExtract;

    @Resource
    CkMetaExtractImpl ckMetaExtract;

    @Resource
    OriginDataExtract originDataExtract;

    @Resource
    DumpTransform dumpTransform;

    @Resource
    RuleConfigParser ruleConfigParser;

    @Resource
    TargetDatasources targetDatasources;

    @Resource
    AlarmConfig alarmConfig;


    /**
     * 1、获取所有要同步的表
     * 2、每一张表检查是否要同步原始数据
     * 3、如果需要同步原始数据，则使用mysqldump开始抽取
     * 4、否则使用使用kafka同步增量数据
     */
    @EventListener(ApplicationReadyEvent.class)
    public void startRiver() {
        ruleConfigParser.initAllRules();
        System.out.println("=======================syncing manifest===========================");
        Map<String, TableMeta> tableRules = RULES_MAP.asMap();
        if (tableRules.size() == 0) {
            log.error("未找到任何的同步任务，请检查配置文件。");
            System.exit(1);
        }
        for (String key : tableRules.keySet()) {
            TableMeta currTableMeta = tableRules.get(key);
            System.out.println("dbName:" + currTableMeta.getDbName() + ",tableName:" + currTableMeta.getTableName() + ",table:" + currTableMeta.getTargetTableName() + ",topicName:" + currTableMeta.getTopicName());
        }
        System.out.println("=======================start river===========================");
        for (String key : tableRules.keySet()) {
            TableMeta currTableMeta = tableRules.get(key);
            Conn conn = DbUtils.getConnByDbId(targetDatasources.getDatasources(), currTableMeta.getTargetDbId());
            try {
                //如果目标数据库是ck，那么要探测目标表的引擎
                if(Conn.DB_TYPE_CLICKHOUSE.equals(conn.getType())){
                    String engineName = ckMetaExtract.getTableEngineName(currTableMeta.getTargetDbId(),currTableMeta.getTargetTableName());
                    currTableMeta.setCkTableEngine(engineName);
                    log.info("find ck tableName:{} engine:{}",currTableMeta.getTargetTableName(),engineName);
                }

                if (Constants.YES.equals(currTableMeta.getSyncConfig().getDumpOriginData())) {
                    AbstractLoadService loadService = AbstractLoadService.getLoadService(conn.getType());
                    //查看目标数据库是否存在且有数据
                    Long targetCount = loadService.countData(currTableMeta.getTargetDbId(), currTableMeta.getTargetTableName());
                    //查看源数据是否有数据
                    Long sourceCount = mysqlMetaExtract.getDataCount(currTableMeta.getSourceDbId(), currTableMeta.getTableName());
                    if (sourceCount > 0 && targetCount == 0) {
                        log.warn("start to dump origin data of " + currTableMeta.getDbName() + "." + currTableMeta.getTableName());
                        currTableMeta.setState(SyncState.LOADING_ORIGIN_DATA);
                        loadService.checkAndCreateStorage(currTableMeta);
                        //开始同步原始数据
                        File dataFile = originDataExtract.dumpData(currTableMeta);
                        Iterator iterator = dumpTransform.transform(dataFile, currTableMeta);
                        while (iterator.hasNext()) {
                            List<CudRequest> requestList = (List<CudRequest>) iterator.next();
                            if (requestList.size() > 0) {
                                loadService.batchAdd(requestList);
                            }
                        }
                        loadService.flushBatchAdd();
                        log.warn("dump origin data is success,tableName:{},dbName:{},esIndex:{},topicName:{}",
                                currTableMeta.getTableName(), currTableMeta.getDbName(), currTableMeta.getTargetTableName(), currTableMeta.getTopicName());
                    } else {
                        log.warn("skip dump origin data,tableName:{},dbName:{},esIndex:{},topicName:{}",
                                currTableMeta.getTableName(), currTableMeta.getDbName(), currTableMeta.getTargetTableName(), currTableMeta.getTopicName());
                    }
                } else {
                    log.warn("config dumpOriginData is false,skip dump origin data,tableName:{},dbName:{},",
                            currTableMeta.getTableName(), currTableMeta.getDbName());
                }


                //等待同步增量数据
                currTableMeta.setState(SyncState.WAIT_TO_LISTENING);
                KafkaMessageListenerContainer container = KafkaConfig
                        .getKafkaListener(currTableMeta.getSourceDbId(), currTableMeta.getTopicGroup(), currTableMeta.getTopicName());
                if (KafkaConfig.canStartListener(container, currTableMeta.getTopicGroup(), currTableMeta.getTopicName())) {
                    //开始同步增量数据
                    log.info("kafka({}) start listening!", container.getBeanName());
                    container.start();
                    List<TableMeta> tableMetaList = RuleConfigParser
                            .getTableMetaListByMq(currTableMeta.getTopicName(), currTableMeta.getTopicGroup());
                    tableMetaList.forEach(t -> {
                        t.setState(SyncState.SYNCING);
                    });
                } else {
                    log.info("failed to start listening!", container.getBeanName());
                }
            } catch (Exception e) {
                log.error("start river is fail,tableName:{},dbName:{},esIndex:{},topicName:{}",
                        currTableMeta.getTableName(), currTableMeta.getDbName(), currTableMeta.getTargetTableName(), currTableMeta.getTopicName(), e);
                //启动报错直接退出
                System.exit(1);
            }
        }

        //开启监控
        alarmConfig.startMonitor();
    }

}

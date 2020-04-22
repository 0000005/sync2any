package com.jte.sync2es.core;

import com.jte.sync2es.conf.KafkaConfig;
import com.jte.sync2es.conf.RuleConfigParser;
import com.jte.sync2es.exception.ShouldNeverHappenException;
import com.jte.sync2es.extract.SourceMetaExtract;
import com.jte.sync2es.extract.SourceOriginDataExtract;
import com.jte.sync2es.load.LoadService;
import com.jte.sync2es.model.core.SyncState;
import com.jte.sync2es.model.es.EsRequest;
import com.jte.sync2es.model.mysql.TableMeta;
import com.jte.sync2es.transform.DumpTransform;
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

import static com.jte.sync2es.conf.RuleConfigParser.RULES_MAP;

/**
 * 在整个spring 容器启动之后，再进行业务上的启动
 */
@Configuration
@Slf4j
public class StartListener {



    @Resource
    SourceMetaExtract sourceMetaExtract;

    @Resource
    LoadService loadService;

    @Resource
    SourceOriginDataExtract sourceOriginDataExtract;

    @Resource
    DumpTransform dumpTransform;

    @Resource
    RuleConfigParser ruleConfigParser;

    /**
     * 1、获取所有要同步的表
     * 2、每一张表检查是否要同步原始数据
     * 3、如果需要同步原始数据，则使用mysqldump开始抽取
     * 4、否则使用使用kafka同步增量数据
     */
    @EventListener(ApplicationReadyEvent.class)
    public void startRiver()
    {
        ruleConfigParser.initRules();
        System.out.println("=======================syncing manifest===========================");
        Map<String, TableMeta> tableRules=RULES_MAP.asMap();
        for(String key:tableRules.keySet())
        {
            TableMeta currTableMeta = tableRules.get(key);
            System.out.println("dbName:"+currTableMeta.getDbName()+",tableName:"+currTableMeta.getTableName()+",esIndex:"+currTableMeta.getEsIndexName()+",topicName:"+currTableMeta.getTopicName());
        }
        System.out.println("=======================start river===========================");
        for(String key:tableRules.keySet())
        {
            TableMeta currTableMeta= tableRules.get(key);
            try
            {
                //查看es的index是否存在且有数据
                Long targetCount=loadService.countData(currTableMeta.getEsIndexName());
                //查看源数据是否有数据
                Long sourceCount=sourceMetaExtract.getDataCount(currTableMeta.getDbName(),currTableMeta.getTableName());
                if(sourceCount>0&&targetCount==0)
                {
                    log.warn("start to dump origin data of "+currTableMeta.getDbName()+"."+currTableMeta.getTableName());
                    currTableMeta.setState(SyncState.LOADING_ORIGIN_DATA);
                    loadService.checkAndCreateStorage(currTableMeta);
                    //开始同步原始数据
                    File dataFile=sourceOriginDataExtract.dumpData(currTableMeta);
                    Iterator iterator=dumpTransform.transform(dataFile,currTableMeta);
                    while(iterator.hasNext())
                    {
                        List<EsRequest> requestList= (List<EsRequest>) iterator.next();
                        if(requestList.size()>0)
                        {
                            long affectCount=loadService.batchAdd(requestList);
                            if(affectCount!=requestList.size())
                            {
                                throw new ShouldNeverHappenException("sync origin data fail! tableName:"+currTableMeta.getTableName()+" esIndex:"+currTableMeta.getEsIndexName());
                            }
                        }
                    }
                    log.warn("dump origin data is success,tableName:{},dbName:{},esIndex:{},topicName:{}",
                            currTableMeta.getTableName(),currTableMeta.getDbName(),currTableMeta.getEsIndexName(),currTableMeta.getTopicName());
                }
                //开始同步增量数据
                currTableMeta.setState(SyncState.SYNCING);
                KafkaMessageListenerContainer container=KafkaConfig
                        .getKafkaListener(currTableMeta.getDbName(),currTableMeta.getTopicGroup(),currTableMeta.getTopicName());
                if(KafkaConfig.canStartListener(container,currTableMeta.getTopicGroup(),currTableMeta.getTopicName())){
                    log.info("kafka({}) start listening!",container.getBeanName());
                    container.start();
                }
            }
            catch (Exception e)
            {
                currTableMeta.setState(SyncState.STOPPED);
                log.error("start river is fail,tableName:{},dbName:{},esIndex:{},topicName:{}",
                        currTableMeta.getTableName(),currTableMeta.getDbName(),currTableMeta.getEsIndexName(),currTableMeta.getTopicName(),e);
            }
        }
    }

}

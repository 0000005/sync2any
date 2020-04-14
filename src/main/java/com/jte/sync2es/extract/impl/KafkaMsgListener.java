package com.jte.sync2es.extract.impl;

import com.jte.sync2es.conf.KafkaConfig;
import com.jte.sync2es.conf.RuleConfigParser;
import com.jte.sync2es.load.LoadService;
import com.jte.sync2es.model.core.SyncState;
import com.jte.sync2es.model.es.EsRequest;
import com.jte.sync2es.model.mq.TcMqMessage;
import com.jte.sync2es.model.mysql.TableMeta;
import com.jte.sync2es.model.mysql.TableRecords;
import com.jte.sync2es.transform.RecordsTransform;
import com.jte.sync2es.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * kafka消息接收器
 */
@Slf4j
public class KafkaMsgListener implements AcknowledgingMessageListener<String,String> {

    public static final String EVENT_TYPE_INSERT = "insert";
    public static final String EVENT_TYPE_UPDATE = "update";
    public static final String EVENT_TYPE_DELETE = "delete";

    private RecordsTransform transform;
    private LoadService load;

    public KafkaMsgListener(RecordsTransform transform, LoadService load) {
        this.load = load;
        this.transform = transform;
    }

    @Override
    public void onMessage(ConsumerRecord<String,String> data, Acknowledgment acknowledgment) {

        System.out.println("message key:"+data.key()+" value:"+data.value());
        TcMqMessage message =JsonUtil.jsonToPojo(data.value(),TcMqMessage.class);
        TableMeta tableMeta=RuleConfigParser.RULES_MAP
                .getIfPresent(message.getDb().toLowerCase()+"$"+message.getTable().toLowerCase());
        if(Objects.isNull(tableMeta))
        {
            log.warn("tableMeta not found when receive msg from mq. this msg will be ignored. msg:{}",data.value());
            return ;
        }
        try
        {
            String eventTypeStr=message.getEventtypestr();
            if(!EVENT_TYPE_DELETE.equals(eventTypeStr)&&!EVENT_TYPE_UPDATE.equals(eventTypeStr)&&!EVENT_TYPE_INSERT.equals(eventTypeStr))
            {
                log.warn("Ignore unsupported event type:{} , message:{}",eventTypeStr,message);
                return ;
            }

            //将mq的信息转为mysql形式，且带有规则信息
            TableRecords tableRecords=TableRecords.buildRecords(tableMeta,message);
            //将tableRecords 转化为es的形式
            EsRequest request=transform.transform(tableRecords);
            //将信息同步到es中
            load.operateData(request);
            //记录时间
            long dataUpdateTime=new BigDecimal(message.getBegintime()).multiply(new BigDecimal(1000)).longValue();
            tableMeta.setLastDataManipulateTime(dataUpdateTime);
            tableMeta.setLastSyncTime(System.currentTimeMillis());
            //at last,we commit this msg.
            acknowledgment.acknowledge();
        }
        catch (Exception e)
        {
            log.error("fatal error! stopping to sync this table '{}'!",tableMeta.getTableName(),e);
            stopListener(data,message);
            tableMeta.setState(SyncState.STOPPED);
        }
    }

    private void stopListener(ConsumerRecord data,TcMqMessage message)
    {
        String containerName = message.getDb().toLowerCase()+"_"+data.topic();
        KafkaMessageListenerContainer container=KafkaConfig.KAFKA_SET.stream()
                .filter(e->e.getBeanName().equals(containerName))
                .findFirst().orElse(null);
        container.stop();
        log.warn("kafka listener '{}' is stopped!",containerName);
    }

}

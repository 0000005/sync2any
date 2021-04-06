package com.jte.sync2any.extract;

import com.google.common.base.Throwables;
import com.jte.sync2any.conf.KafkaConfig;
import com.jte.sync2any.conf.RuleConfigParser;
import com.jte.sync2any.load.LoadService;
import com.jte.sync2any.model.core.SyncState;
import com.jte.sync2any.model.es.EsRequest;
import com.jte.sync2any.model.mq.TcMqMessage;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.model.mysql.TableRecords;
import com.jte.sync2any.transform.RecordsTransform;
import com.jte.sync2any.util.JsonUtil;
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
    public static final int MAX_RETRY_TIMES =3;

    private RecordsTransform transform;
    private LoadService load;

    public KafkaMsgListener(RecordsTransform transform, LoadService load) {
        this.load = load;
        this.transform = transform;
    }

    @Override
    public void onMessage(ConsumerRecord<String,String> data, Acknowledgment acknowledgment) {

        TableMeta tableMeta = new TableMeta();
        try
        {
            String topicName=data.topic();
            long startTime=System.currentTimeMillis();
            log.debug("message key:"+data.key()+" value:"+data.value());
            TcMqMessage message =JsonUtil.jsonToPojo(data.value(),TcMqMessage.class);
            tableMeta=RuleConfigParser.RULES_MAP
                    .getIfPresent(message.getDb().toLowerCase()+"$"+message.getTable().toLowerCase());
            if(Objects.isNull(tableMeta))
            {
                log.warn("tableMeta not found when receive msg from mq. this msg will be ignored. msg:{}",data.value());
                return ;
            }

            String eventTypeStr=message.getEventtypestr();
            if(!EVENT_TYPE_DELETE.equals(eventTypeStr)&&!EVENT_TYPE_UPDATE.equals(eventTypeStr)&&!EVENT_TYPE_INSERT.equals(eventTypeStr))
            {
                log.warn("Ignore unsupported event type:{} , message:{}",eventTypeStr,message);
                acknowledgment.acknowledge();
                return ;
            }

            //将mq的信息转为mysql形式，且带有规则信息
            TableRecords tableRecords=TableRecords.buildRecords(tableMeta,message);
            //将tableRecords 转化为es的形式
            EsRequest request=transform.transform(tableRecords);
            //将信息同步到es中，如果失败，则重试3次

            for(int i =1;i<=MAX_RETRY_TIMES;i++)
            {
                try
                {
                    load.operateData(request);
                    //如果没问题，则只执行一次
                    break;
                }
                catch (Exception esEx)
                {
                    log.error("load data error,retry times count:"+i,esEx);
                    if(i==3)
                    {
                        throw esEx;
                    }
                }
            }

            //记录时间
            long dataUpdateTime=new BigDecimal(message.getBegintime()).multiply(new BigDecimal(1000)).longValue();
            tableMeta.setLastDataManipulateTime(dataUpdateTime);
            long endTime=System.currentTimeMillis();
            tableMeta.setLastSyncTime(endTime);
            tableMeta.setTpq(endTime-startTime);
            //at last,we commit this msg.
            acknowledgment.acknowledge();
        }
        catch (Exception e)
        {
            log.error("fatal error! stopping to sync this table '{}',data:{}!",tableMeta.getTableName(),data,e);
            stopListener(tableMeta,e);
        }
    }

    public static void stopListener(TableMeta tableMeta,Exception e)
    {
        if(Objects.isNull(tableMeta.getDbName()))
        {
            return;
        }
        tableMeta.setState(SyncState.STOPPED);
        tableMeta.setErrorReason(Throwables.getStackTraceAsString(e));
        KafkaMessageListenerContainer container=KafkaConfig
                .getKafkaListener(tableMeta.getDbName(),tableMeta.getTopicGroup(),tableMeta.getTopicName());
        if(container.isRunning())
        {
            container.stop();
        }
        log.warn("kafka listener '{}' is stopped!",container.getBeanName());
    }

}

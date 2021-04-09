package com.jte.sync2any.extract;

import com.google.common.base.Throwables;
import com.jte.sync2any.conf.KafkaConfig;
import com.jte.sync2any.conf.RuleConfigParser;
import com.jte.sync2any.conf.SpringContextUtils;
import com.jte.sync2any.load.AbstractLoadService;
import com.jte.sync2any.model.config.Conn;
import com.jte.sync2any.model.config.Sync2any;
import com.jte.sync2any.model.config.SyncConfig;
import com.jte.sync2any.model.core.SyncState;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mq.SubscribeDataProto;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.model.mysql.TableRecords;
import com.jte.sync2any.transform.RecordsTransform;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Objects;
import java.util.TimeZone;

import static com.jte.sync2any.model.mq.SubscribeDataProto.DMLType.*;

/**
 * kafka消息接收器
 */
@Slf4j
public class KafkaMsgListener implements AcknowledgingMessageListener<String,byte[]> {

    public static final String EVENT_TYPE_INSERT = "insert";
    public static final String EVENT_TYPE_UPDATE = "update";
    public static final String EVENT_TYPE_DELETE = "delete";
    public static final int MAX_RETRY_TIMES =3;

    private Sync2any sync2any;
    private RecordsTransform transform;

    /**
     * 同一个分区（kafka）的同一个分片(tdsql)中 序号（partitionSeq） 是单调递增且连续的。
     * key: shard_id$partition_id
     * value: partitionSeq
     */
    private HashMap<String, Long> lastSeqMap = new HashMap<>();

    private HashMap<String, ByteArrayOutputStream> shardMsgMap = new HashMap<>();

    public KafkaMsgListener(RecordsTransform transform, Sync2any sync2any)
    {
        this.transform = transform;
        this.sync2any = sync2any;
    }

    @Override
    public void onMessage(ConsumerRecord<String,byte[]> data, Acknowledgment acknowledgment)
    {

        SubscribeDataProto.Entries entries = parseFromMq(data);
        if(Objects.isNull(entries))
        {
            return ;
        }

        for (SubscribeDataProto.Entry entry : entries.getItemsList()) {
            processEntry(data,entry);
        }

        // commit at checkpoint message
        if (entries.getItemsCount() > 0 && entries.getItems(0).getHeader().getMessageType() == SubscribeDataProto.MessageType.CHECKPOINT) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 处理每一行记录
     * @param data
     * @param entry
     */
    private void processEntry(ConsumerRecord<String,byte[]> data,SubscribeDataProto.Entry entry) {
        try
        {
            SubscribeDataProto.Header header = entry.getHeader();
            log.debug("-->[kafka partition: {}, kafka offset: {}, partitionSeq: {}] [{}], binlog timestamp: {}",
                    data.partition(),
                    data.offset(),
                    getPartitionSeq(data),
                    header.getFileName(), header.getPosition(),
                    LocalDateTime.ofInstant(Instant.ofEpochSecond(header.getTimestamp()),TimeZone.getDefault().toZoneId()));


            SubscribeDataProto.MessageType messageType=entry.getHeader().getMessageType();
            if(!SubscribeDataProto.MessageType.DML.equals(messageType))
            {
                log.info("Ignore unsupported message type:{}",messageType.toString());
                return ;
            }

            SubscribeDataProto.DMLEvent dmlEvt = entry.getEvent().getDmlEvent();
            SubscribeDataProto.DMLType dmlType = dmlEvt.getDmlEventType();
            if(!dmlType.equals(INSERT)&&!dmlType.equals(UPDATE)&&!dmlType.equals(DELETE))
            {
                log.info("Ignore unsupported dml event type:{}",dmlType.toString());
                return ;
            }
            String dbName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();

            long startTime=System.currentTimeMillis();
            String topicName=data.topic();
            //从topicName中匹配源数据库
            SyncConfig syncConfig=sync2any.findSyncConfigByTopicName(topicName);

            TableMeta tableMeta=RuleConfigParser.RULES_MAP
                    .getIfPresent(syncConfig.getSourceDbId()+"$"+tableName.toLowerCase());
            if(Objects.isNull(tableMeta))
            {
                log.warn("tableMeta not found when receive msg from mq. this msg will be ignored. db:{}.{}",dbName,tableName);
                return ;
            }

            for (SubscribeDataProto.RowChange row : dmlEvt.getRowsList())
            {

                //将mq的信息转为mysql形式，且已经进行了规则的处理
                TableRecords tableRecords=TableRecords.buildRecords(tableMeta,row,dmlEvt);
                //将tableRecords 转化为可操作的形式
                AbstractLoadService loadService;
                if(Conn.DB_TYPE_ES.equals(syncConfig.getTargetType()))
                {
                    loadService = (AbstractLoadService) SpringContextUtils.getContext().getBean("esLoadServiceImpl");
                }
                else if(Conn.DB_TYPE_MYSQL.equals(syncConfig.getTargetType()))
                {
                    loadService = (AbstractLoadService) SpringContextUtils.getContext().getBean("mysqlLoadServiceImpl");
                }
                else
                {
                    log.error("同步过程中发现错误的目标类型（targetType）:{}，请检查配置文件！",syncConfig.getTargetType());
                    return ;
                }

                CudRequest request=transform.transform(tableRecords);

                if(Objects.isNull(request))
                {
                    log.error("CudRequest 为null,mq:{}",data.value());
                }

                //将信息同步到es中，如果失败，则重试3次
                for(int i =1;i<=MAX_RETRY_TIMES && !Objects.isNull(request); i++)
                {
                    try
                    {
                        loadService.operateData(request);
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
                updateStatics(tableMeta,header.getTimestamp(),startTime);
            }
        }
        catch (Exception e)
        {
            //TODO 触发告警
            log.error("处理消息失败，topic:{},offset:{},partition:{}",data.topic(),data.offset(),data.partition(),e);
        }

    }

    /**
     * 将mq中的字节流消息转化为proto的形式
     * @param data
     * @return
     * @throws IOException
     */
    public SubscribeDataProto.Entries parseFromMq(ConsumerRecord<String,byte[]> data){

        SubscribeDataProto.Entries entries=null;
        try
        {
            //kafka 分区号
            long partitionId = data.partition();
            //消息序号
            long partitionSeq = getPartitionSeq(data);
            //数据库分片id
            String shardId = getShardId(data);

            String positionKey=shardId+"$"+partitionId;
            Long shardLastSeq = lastSeqMap.get(positionKey);

            //初始化消息序号
            if (shardLastSeq == null || partitionSeq == 1) {
                lastSeqMap.put(positionKey, (long) 1);
            } else if (partitionSeq <= shardLastSeq) {
                log.warn("重复的消息: " + data);
                return null;
            } else if (partitionSeq != shardLastSeq + 1 && shardLastSeq != 0) {
                throw new IllegalStateException("消息序号不连续, last: " + shardLastSeq + ", current: " + partitionSeq);
            }
            //缓存最新的消息序号
            lastSeqMap.put(positionKey, partitionSeq);

            SubscribeDataProto.Envelope envelope = SubscribeDataProto.Envelope.parseFrom(data.value());
            if (1 != envelope.getVersion()) {
                throw new IllegalStateException(String.format("unsupported version: %d", envelope.getVersion()));
            }
            ByteArrayOutputStream completeMsg = new ByteArrayOutputStream();
            if (1 == envelope.getTotal()) {
                completeMsg = new ByteArrayOutputStream();
                envelope.getData().writeTo(completeMsg);
            } else {
                ByteArrayOutputStream shardMsg = shardMsgMap.get(positionKey);
                if (null == shardMsg) {
                    shardMsg = new ByteArrayOutputStream();
                    envelope.getData().writeTo(shardMsg);
                    shardMsgMap.put(positionKey, shardMsg);
                } else {
                    envelope.getData().writeTo(shardMsg);
                }
            }

            if (envelope.getIndex() < envelope.getTotal() - 1) {
                log.warn("本次接受到的数据包不完整，继续拼装数据。index:{} , total:{}",envelope.getIndex(),envelope.getTotal());
                return null;
            }

            if (1 == envelope.getTotal()) {
                entries = SubscribeDataProto.Entries.parseFrom(completeMsg.toByteArray());
            } else {
                entries = SubscribeDataProto.Entries.parseFrom(shardMsgMap.get(shardId).toByteArray());
            }
        }
        catch (Exception e)
        {
            //TODO 触发告警
            log.error("解析mq消息失败，topic:{},offset:{},partition:{}",data.topic(),data.offset(),data.partition(),e);
        }

        return entries;
    }

    /**
     * 更新统计信息
     * @param tableMeta
     * @param time
     * @param startTime
     */
    private void updateStatics(TableMeta tableMeta,int time,long startTime){
        long dataUpdateTime=new BigDecimal(time).multiply(new BigDecimal(1000)).longValue();
        tableMeta.setLastDataManipulateTime(dataUpdateTime);
        long endTime=System.currentTimeMillis();
        tableMeta.setLastSyncTime(endTime);
        tableMeta.setTpq(endTime-startTime);
    }


    private final static String partitionSeqKey = "ps";

    private static long getPartitionSeq(ConsumerRecord<String, byte[]> msg) {
        Iterator<Header> it = msg.headers().headers(partitionSeqKey).iterator();
        if (it.hasNext()) {
            Header h = it.next();
            return Long.parseUnsignedLong(new String(h.value()));
        }

        throw new IllegalStateException(partitionSeqKey + " does not exists");
    }


    private final static String ShardIdKey = "ShardId";

    private static String getShardId(ConsumerRecord<String, byte[]> msg) {
        Iterator<Header> it = msg.headers().headers(ShardIdKey).iterator();
        if (it.hasNext()) {
            Header h = it.next();
            return new String(h.value());
        }

        throw new IllegalStateException(ShardIdKey + " does not exists");
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

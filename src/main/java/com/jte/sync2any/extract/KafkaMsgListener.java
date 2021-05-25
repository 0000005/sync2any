package com.jte.sync2any.extract;

import com.google.common.base.Throwables;
import com.jte.sync2any.MonitorTask;
import com.jte.sync2any.conf.KafkaConfig;
import com.jte.sync2any.conf.RuleConfigParser;
import com.jte.sync2any.load.AbstractLoadService;
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
import java.util.*;

import static com.jte.sync2any.model.mq.SubscribeDataProto.DMLType.*;

/**
 * kafka消息接收器
 */
@Slf4j
public class KafkaMsgListener implements AcknowledgingMessageListener<String, byte[]> {

    public static final int MAX_RETRY_TIMES = 3;

    private Sync2any sync2any;
    private RecordsTransform transform;
    private RuleConfigParser ruleConfigParser;

    /**
     * 同一个分区（kafka）的同一个分片(tdsql)中 序号（partitionSeq） 是单调递增且连续的。
     * key: shard_id$partition_id
     * value: partitionSeq
     */
    private HashMap<String, Long> lastSeqMap = new HashMap<>();

    private HashMap<String, ByteArrayOutputStream> shardMsgMap = new HashMap<>();

    public KafkaMsgListener(RecordsTransform transform, Sync2any sync2any, RuleConfigParser ruleConfigParser) {
        this.transform = transform;
        this.sync2any = sync2any;
        this.ruleConfigParser = ruleConfigParser;
    }

    @Override
    public void onMessage(ConsumerRecord<String, byte[]> data, Acknowledgment acknowledgment) {
        try {
            SubscribeDataProto.Entries entries = parseFromMq(data);
            if (Objects.isNull(entries)) {
                return;
            }

            for (SubscribeDataProto.Entry entry : entries.getItemsList()) {
                processEntry(data, entry);
            }

            // commit at checkpoint message
            if (entries.getItemsCount() > 0 && entries.getItems(0).getHeader().getMessageType() == SubscribeDataProto.MessageType.CHECKPOINT) {
                acknowledgment.acknowledge();
            }
        } catch (Exception e) {
            MonitorTask.ERROR_TOPIC_LIST.add(data.topic());
            log.error("syncing data error:", e);
        }

    }

    /**
     * 处理每一行记录
     *
     * @param data
     * @param entry
     */
    private void processEntry(ConsumerRecord<String, byte[]> data, SubscribeDataProto.Entry entry) {
        try {
            SubscribeDataProto.Header header = entry.getHeader();
            log.debug("-->[kafka partition: {}, kafka offset: {}, partitionSeq: {}] [{}], binlog timestamp: {}",
                    data.partition(),
                    data.offset(),
                    getPartitionSeq(data),
                    header.getFileName(), header.getPosition(),
                    LocalDateTime.ofInstant(Instant.ofEpochSecond(header.getTimestamp()), TimeZone.getDefault().toZoneId()));


            SubscribeDataProto.MessageType messageType = entry.getHeader().getMessageType();
            if (!SubscribeDataProto.MessageType.DML.equals(messageType)) {
                if (SubscribeDataProto.MessageType.DDL.equals(messageType)) {
                    //发现新表，尝试添加到RULES_MAP中。
                    SubscribeDataProto.DDLEvent ddlEvent = entry.getEvent().getDdlEvent();
                    log.warn("new ddl shardId:{} , schema:{} , sql:{} , RULES_MAP size:{}", getShardId(data), ddlEvent.getSchemaName(), ddlEvent.getSql(), RuleConfigParser.RULES_MAP.size());
                    String newTableName = RuleConfigParser.getTableNameFromDdl(ddlEvent.getSql());
                    List<SyncConfig> configList = ruleConfigParser.getSysConfigBySourceDbName(ddlEvent.getSchemaName());
                    if (Objects.nonNull(configList) && configList.size() > 0) {
                        List<TableMeta> tableMetaList = ruleConfigParser.initRules(configList, newTableName);
                        tableMetaList.forEach(t->{
                            t.setState(SyncState.SYNCING);
                        });
                    }
                    log.warn("configListSize:{} , newTableName:{} , RULES_MAP size:{}", configList.size(), newTableName, RuleConfigParser.RULES_MAP.size());
                    return;
                } else if (SubscribeDataProto.MessageType.BEGIN.equals(messageType) || SubscribeDataProto.MessageType.COMMIT.equals(messageType) || SubscribeDataProto.MessageType.CHECKPOINT.equals(messageType)) {
                    //不打印begin\commit\checkpoint，日志太多了。
                    return;
                }
                //非增删改语句
                log.info("Ignore unsupported message type:{}", messageType.toString());
                return;
            }

            SubscribeDataProto.DMLEvent dmlEvt = entry.getEvent().getDmlEvent();
            SubscribeDataProto.DMLType dmlType = dmlEvt.getDmlEventType();
            if (!dmlType.equals(INSERT) && !dmlType.equals(UPDATE) && !dmlType.equals(DELETE)) {
                //非增删改语句
                log.info("Ignore unsupported dml event type:{}", dmlType.toString());
                return;
            }

            String dbName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();

            long startTime = System.currentTimeMillis();
            String topicName = data.topic();

            log.debug("receive data change dmlType:{},dbName:{},tableName{},topicName{},rowSize:{}"
                    , dmlType.toString(), dbName, tableName, topicName, dmlEvt.getRowsList().size());

            //从topicName中匹配源数据库
            SyncConfig syncConfig = sync2any.findSyncConfigByTopicName(topicName);

            TableMeta tableMeta = RuleConfigParser.RULES_MAP
                    .getIfPresent(syncConfig.getSourceDbId() + "$" + tableName.toLowerCase());
            if (Objects.isNull(tableMeta)) {
                log.warn("tableMeta not found when receive msg from mq. this msg will be ignored. db:{}.{}", dbName, tableName);
                return;
            }

            for (SubscribeDataProto.RowChange row : dmlEvt.getRowsList()) {

                //将mq的信息转为mysql形式，且已经进行了规则的处理
                TableRecords tableRecords = TableRecords.buildRecords(tableMeta, row, dmlEvt);
                //将tableRecords 转化为可操作的形式
                AbstractLoadService loadService = AbstractLoadService.getLoadService(syncConfig.getTargetType());
                CudRequest request = transform.transform(tableRecords);

                if (Objects.isNull(request)) {
                    log.error("CudRequest 为null,mq:{}", data.value());
                }

                //将信息同步到目标数据库中中，如果失败，则重试3次
                for (int i = 1; i <= MAX_RETRY_TIMES && !Objects.isNull(request); i++) {
                    try {
                        int effectNum = loadService.operateData(request);
                        log.debug("tableName:{},effect number:{}", tableName, effectNum);
                        //如果没问题，则只执行一次
                        break;
                    } catch (Exception esEx) {
                        log.error("load data error,retry times count:" + i, esEx);
                        if (i == 3) {
                            throw esEx;
                        }
                    }
                }
                updateStatics(tableMeta, header.getTimestamp(), startTime, data.offset());
            }
        } catch (Exception e) {
            //触发告警
            MonitorTask.ERROR_TOPIC_LIST.add(data.topic());
            log.error("处理消息失败，topic:{},offset:{},partition:{}", data.topic(), data.offset(), data.partition(), e);
        }

    }

    /**
     * 将mq中的字节流消息转化为proto的形式
     *
     * @param data
     * @return
     * @throws IOException
     */
    public SubscribeDataProto.Entries parseFromMq(ConsumerRecord<String, byte[]> data) throws Exception {

        SubscribeDataProto.Entries entries = null;
        //kafka 分区号
        long partitionId = data.partition();
        //消息序号
        long partitionSeq = getPartitionSeq(data);
        //数据库分片id
        String shardId = getShardId(data);

        Long shardLastSeq = lastSeqMap.get(shardId);

        //初始化消息序号
        if (shardLastSeq == null || partitionSeq == 1) {
            lastSeqMap.put(shardId, (long) 1);
        } else if (partitionSeq <= shardLastSeq) {
            log.warn("重复的消息: " + data);
            return null;
        } else if (partitionSeq != shardLastSeq + 1 && shardLastSeq != 0) {
            throw new IllegalStateException("消息序号不连续, last: " + shardLastSeq + ", current: " + partitionSeq);
        }
        //缓存最新的消息序号
        lastSeqMap.put(shardId, partitionSeq);

        SubscribeDataProto.Envelope envelope = SubscribeDataProto.Envelope.parseFrom(data.value());
        if (1 != envelope.getVersion()) {
            throw new IllegalStateException(String.format("unsupported version: %d", envelope.getVersion()));
        }
        ByteArrayOutputStream completeMsg = new ByteArrayOutputStream();
        if (1 == envelope.getTotal()) {
            completeMsg = new ByteArrayOutputStream();
            envelope.getData().writeTo(completeMsg);
        } else {
            ByteArrayOutputStream shardMsg = shardMsgMap.get(shardId);
            if (null == shardMsg) {
                shardMsg = new ByteArrayOutputStream();
                envelope.getData().writeTo(shardMsg);
                shardMsgMap.put(shardId, shardMsg);
            } else {
                envelope.getData().writeTo(shardMsg);
            }
        }

        if (envelope.getIndex() < envelope.getTotal() - 1) {
            log.warn("本次接受到的数据包不完整，继续拼装数据。index:{} , total:{} , shardId:{} , shardMsgExits:{}", envelope.getIndex(), envelope.getTotal(), shardId, shardMsgMap.get(shardId) == null);
            return null;
        }

        if (1 == envelope.getTotal()) {
            entries = SubscribeDataProto.Entries.parseFrom(completeMsg.toByteArray());
        } else {
            log.warn("多个数据包合并解析。index:{} , total:{} , shardId:{} , shardMsgExits:{}", envelope.getIndex(), envelope.getTotal(), shardId, Objects.nonNull(shardMsgMap.get(shardId)) ? "true" : "false");
            entries = SubscribeDataProto.Entries.parseFrom(shardMsgMap.get(shardId).toByteArray());
            shardMsgMap.remove(shardId);
        }

        return entries;
    }

    /**
     * 更新统计信息
     *
     * @param tableMeta
     * @param time
     * @param startTime
     */
    private void updateStatics(TableMeta tableMeta, int time, long startTime, long offset) {
        long dataUpdateTime = new BigDecimal(time).multiply(new BigDecimal(1000)).longValue();
        tableMeta.setLastDataManipulateTime(dataUpdateTime);
        long endTime = System.currentTimeMillis();
        tableMeta.setLastSyncTime(endTime);
        tableMeta.setTpq(endTime - startTime);
        tableMeta.setLastOffset(offset);
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


    public static KafkaMessageListenerContainer stopListener(String topicName, String topicGroup, String sourceDbId, Exception e) {

        //找到这个topicName 和 topicGroup对应的所有table,设置为停止
        List<TableMeta> tableMetaList = RuleConfigParser.getTableMetaListByMq(topicName, topicGroup);
        tableMetaList.forEach(t -> {
            t.setState(SyncState.STOPPED);
            t.setErrorReason(Throwables.getStackTraceAsString(e));
        });

        KafkaMessageListenerContainer container = KafkaConfig
                .getKafkaListener(sourceDbId, topicGroup, topicName);
        if (Objects.nonNull(container) && container.isRunning()) {
            container.stop();
            log.warn("kafka listener '{}' is stopped!", container.getBeanName());
        } else {
            log.warn("kafka listener '{}' is not running, skip stop action!", container.getBeanName());
        }
        return container;
    }


}

package com.jte.sync2any.conf;

import com.jte.sync2any.extract.KafkaMsgListener;
import com.jte.sync2any.model.config.KafkaMate;
import com.jte.sync2any.model.config.Mq;
import com.jte.sync2any.model.config.Sync2any;
import com.jte.sync2any.model.core.SyncState;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.transform.RecordsTransform;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;

import static com.jte.sync2any.conf.RuleConfigParser.RULES_MAP;

@Configuration
@Slf4j
public class KafkaConfig {
    @Resource
    KafkaMate kafkaMate;
    @Resource
    Sync2any sync2any;
    @Resource
    RecordsTransform transform;

    public static final Set<KafkaMessageListenerContainer> KAFKA_SET = new HashSet<>();

    @PostConstruct
    public void initKafka() {
        if (StringUtils.isBlank(kafkaMate.getAddress())) {
            log.error("请填写kafka的相关配置。");
            System.exit(500);
        }
        sync2any.getSyncConfigList().forEach(sdb -> {
            KafkaMessageListenerContainer<String, byte[]> container = createContainer(sdb.getMq());
            container.setBeanName(sdb.getSourceDbId().toLowerCase() + "_" + sdb.getMq().getTopicGroup() + "_" + sdb.getMq().getTopicName());
            KAFKA_SET.add(container);
        });
    }


    private KafkaMessageListenerContainer<String, byte[]> createContainer(Mq mq) {
        return createContainer(mq, null);
    }

    public KafkaMessageListenerContainer<String, byte[]> createContainer(Mq mq, Long offset) {

        ContainerProperties containerProps = null;
        if (Objects.isNull(offset)) {
            containerProps = new ContainerProperties(mq.getTopicName());
        } else {
            //设置指定的offset
            TopicPartitionOffset partitionOffset = new TopicPartitionOffset(mq.getTopicName(), 0, offset);
            containerProps = new ContainerProperties(partitionOffset);
        }

        //手动控制提交，每消费完一条消息就提交一次
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        //异步提交
        containerProps.setSyncCommits(false);
        containerProps.setMessageListener(new KafkaMsgListener(transform, sync2any));

        Map<String, Object> props = consumerProps(mq);
        DefaultKafkaConsumerFactory<String, byte[]> cf = new DefaultKafkaConsumerFactory<>(props);
        KafkaMessageListenerContainer<String, byte[]> container = new KafkaMessageListenerContainer<>(cf, containerProps);

        container.setAutoStartup(false);
        return container;
    }

    private Map<String, Object> consumerProps(Mq mq) {
        Map<String, Object> props = new HashMap<>(20);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaMate.getAddress());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, mq.getTopicGroup());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        //只读取已经提交的消息
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        //https://blog.csdn.net/a953713428/article/details/80030893
        //每次fetch 3MB的数据到本地
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 3 * 1024 * 1024);
        //每次最多本地缓存拉取600条数据,600条数据必须在MAX_POLL_INTERVAL_MS_CONFIG时间内消费完成。
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 600);
        //1次拉去最多处理时间为300秒
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 5000);

        //鉴权信息
        if (StringUtils.isNotBlank(mq.getUsername())) {
            log.info("set up kafka authentication, username:{}", mq.getUsername());
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
            props.put(SaslConfigs.SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                            "  username=\"" + mq.getUsername() + "\"" +
                            "  password=\"" + mq.getPassword() + "\";");
        }
        return props;
    }

    public static KafkaMessageListenerContainer getKafkaListener(String dbId, String topicGroup, String topicName) {
        String beanName = dbId + "_" + topicGroup + "_" + topicName;
        return KAFKA_SET.stream().filter(k -> k.getBeanName().equals(beanName)).findFirst().orElse(null);
    }

    /**
     * 如果多个表在同一个topic（同group）内同步的情况
     * 那么必须当最后一个表dump了之后，才可以开启这个topic的监听
     *
     * @param container
     * @param topicGroup
     * @param topicName
     * @return
     */
    public static boolean canStartListener(KafkaMessageListenerContainer container, String topicGroup, String topicName) {
        if (container.isRunning()) {
            log.warn("Should never happen! Listener container SHOULD NOT BE start more than once. container name:" + container.getBeanName());
            return false;
        }
        //找到所有同topicGroup、topicName的Rule
        Map<String, TableMeta> tableRules = RULES_MAP.asMap();
        //查找还未准备好的table的数量
        long notReadyCont = tableRules.keySet().stream()
                .filter(key -> {
                    TableMeta meta = tableRules.get(key);
                    return topicName.equals(meta.getTopicName()) && topicGroup.equals(meta.getTopicGroup());
                })
                .filter(key -> {
                    TableMeta meta = tableRules.get(key);
                    return !SyncState.SYNCING.equals(meta.getState());
                }).count();
        //一旦还有未准备好的table，就不能去同步
        return notReadyCont == 0;
    }
}

package com.jte.sync2any.conf;

import com.jte.sync2any.extract.impl.KafkaMsgListener;
import com.jte.sync2any.load.LoadService;
import com.jte.sync2any.model.config.KafkaMate;
import com.jte.sync2any.model.config.Mq;
import com.jte.sync2any.model.config.Sync2any;
import com.jte.sync2any.model.core.SyncState;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.transform.RecordsTransform;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    @Resource
    LoadService load;


    public static final Set<KafkaMessageListenerContainer> KAFKA_SET= new HashSet<>();

    @PostConstruct
    public void initKafka() {
        if(StringUtils.isBlank(kafkaMate.getAdress()))
        {
            log.error("请填写kafka的相关配置。");
            System.exit(500);
        }
        sync2any.getSyncConfigList().forEach(sdb->{
            ContainerProperties containerProps = new ContainerProperties(sdb.getMq().getTopicName());
            containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            containerProps.setMessageListener(new KafkaMsgListener(transform,load));
            KafkaMessageListenerContainer<String, String> container = createContainer(sdb.getMq(),containerProps);
            container.setBeanName(sdb.getDbName().toLowerCase()+"_"+sdb.getMq().getTopicGroup()+"_"+sdb.getMq().getTopicName());
            KAFKA_SET.add(container);
        });
    }


    private KafkaMessageListenerContainer<String, String> createContainer(Mq mq,ContainerProperties containerProps) {
        Map<String, Object> props = consumerProps(mq);
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<String, String>(props);
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(cf, containerProps);
        return container;
    }

    private Map<String, Object> consumerProps(Mq mq) {
        Map<String, Object> props = new HashMap<>(20);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaMate.getAdress());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, mq.getTopicGroup());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        //每次最多拉取4000条数据
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 4000);
        //1次拉去最多处理时间为300秒
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 5000);
        return props;
    }

    public static KafkaMessageListenerContainer getKafkaListener(String dbName,String topicGroup,String topicName){
        String beanName=dbName.toLowerCase()+"_"+topicGroup+"_"+topicName;
        return KAFKA_SET.stream().filter(k->k.getBeanName().equals(beanName)).findFirst().orElse(null);
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
    public static boolean canStartListener(KafkaMessageListenerContainer container,String topicGroup,String topicName){
        if(container.isRunning())
        {
            log.warn("Should never happen! Listener container SHOULD NOT BE start more than once. container name:"+container.getBeanName());
            return false;
        }
        //找到所有同topicGroup、topicName的Rule
        Map<String, TableMeta> tableRules=RULES_MAP.asMap();
        //查找还未准备好的table的数量
        long notReadyCont=tableRules.keySet().stream()
                .filter(key->{
                    TableMeta meta=tableRules.get(key);
                    return topicName.equals(meta.getTopicName())&&topicGroup.equals(meta.getTopicGroup());
                })
                .filter(key->{
                    TableMeta meta=tableRules.get(key);
                    return !SyncState.SYNCING.equals(meta.getState());
                }).count();
        //一旦还有未准备好的table，就不能去同步
        return notReadyCont==0;
    }
}

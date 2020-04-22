package com.jte.sync2es.conf;

import com.jte.sync2es.extract.impl.KafkaMsgListener;
import com.jte.sync2es.load.LoadService;
import com.jte.sync2es.model.config.KafkaMate;
import com.jte.sync2es.model.config.Mq;
import com.jte.sync2es.model.config.Sync2es;
import com.jte.sync2es.model.core.SyncState;
import com.jte.sync2es.model.mysql.TableMeta;
import com.jte.sync2es.transform.RecordsTransform;
import lombok.extern.slf4j.Slf4j;
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

import static com.jte.sync2es.conf.RuleConfigParser.RULES_MAP;

@Configuration
@Slf4j
public class KafkaConfig {
    @Resource
    KafkaMate kafkaMate;
    @Resource
    Sync2es sync2es;
    @Resource
    RecordsTransform transform;
    @Resource
    LoadService load;


    public static final Set<KafkaMessageListenerContainer> KAFKA_SET= new HashSet<>();

    @PostConstruct
    public void initKafka() {
        sync2es.getSyncConfigList().forEach(sdb->{
            ContainerProperties containerProps = new ContainerProperties(sdb.getMq().getTopicName());
            containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            containerProps.setMessageListener(new KafkaMsgListener(transform,load));
            KafkaMessageListenerContainer<String, String> container = createContainer(sdb.getMq(),containerProps);
            container.setBeanName(sdb.getDbName()+"_"+sdb.getMq().getTopicGroup()+"_"+sdb.getMq().getTopicName());
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
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaMate.getAdress());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, mq.getTopicGroup());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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

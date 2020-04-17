package com.jte.sync2es.conf;

import com.jte.sync2es.extract.impl.KafkaMsgListener;
import com.jte.sync2es.load.LoadService;
import com.jte.sync2es.model.config.KafkaMate;
import com.jte.sync2es.model.config.Mq;
import com.jte.sync2es.model.config.Sync2es;
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
            container.start();
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
        return props;
    }

    public static KafkaMessageListenerContainer getKafkaListener(String dbName,String topicGroup,String topicName){
        String beanName=dbName+"_"+topicGroup+"_"+topicName;
        return KAFKA_SET.stream().filter(k->k.getBeanName().equals(beanName)).findFirst().orElse(null);
    }

}

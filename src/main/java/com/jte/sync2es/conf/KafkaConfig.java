package com.jte.sync2es.conf;

import com.jte.sync2es.model.config.KafkaMate;
import com.jte.sync2es.model.config.Mq;
import com.jte.sync2es.model.config.Sync2es;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.Acknowledgment;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaConfig {
    @Resource
    KafkaMate kafkaMate;
    @Resource
    Sync2es sync2es;
    public static List<KafkaMessageListenerContainer> kafkaList= new ArrayList();

    @PostConstruct
    public void initKafka() {
        sync2es.getSyncConfig().forEach(sdb->{
            ContainerProperties containerProps = new ContainerProperties(sdb.getMq().getTopicName());
            containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            containerProps.setMessageListener(new AcknowledgingMessageListener<String, String>() {
                @Override
                public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
                    System.out.println("message key:"+data.key()+" value:"+data.value());
                    acknowledgment.acknowledge();
                }
            });
            KafkaMessageListenerContainer<String, String> container = createContainer(sdb.getMq(),containerProps);
            container.setBeanName(sdb.getDbId()+"_"+sdb.getMq().getTopicName());
            container.start();
            kafkaList.add(container);
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

}

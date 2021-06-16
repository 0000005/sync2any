package com.jte.sync2any;

import com.jte.sync2any.conf.KafkaConfig;
import com.jte.sync2any.load.impl.CkLoadServiceImpl;
import com.jte.sync2any.model.config.Sync2any;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchDataAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author JerryYin
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, ElasticsearchDataAutoConfiguration.class, ElasticsearchAutoConfiguration.class})
@Configuration
@ConfigurationPropertiesScan
@EnableConfigurationProperties(Sync2any.class)
@Slf4j
public class Sync2anyApplication {

    public static void main(String[] args) {

        SpringApplication.run(Sync2anyApplication.class, args);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdownHook") {
            @Override
            public void run() {
                KafkaConfig.KAFKA_SET.stream().forEach(c -> {
                    if (c.isRunning()) {
                        c.stop();
                        log.warn("kafka listener '{}' is stopped!", c.getBeanName());
                    }
                });
                int i = 0;
                while (i < 10){
                    if(CkLoadServiceImpl.isSaveQueueEmpty()){
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        log.error("wait saveQueue empty.",e);
                    }
                    i++;
                }
            }
        });
    }


}

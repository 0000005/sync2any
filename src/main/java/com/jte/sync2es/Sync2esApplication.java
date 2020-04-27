package com.jte.sync2es;

import com.jte.sync2es.conf.KafkaConfig;
import com.jte.sync2es.model.config.Sync2es;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@Configuration
@ConfigurationPropertiesScan
@EnableConfigurationProperties(Sync2es.class)
@Slf4j
public class Sync2esApplication {

	public static void main(String[] args) {
		SpringApplication.run(Sync2esApplication.class, args);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                KafkaConfig.KAFKA_SET.stream().forEach(c->{
                    if(c.isRunning())
                    {
                        c.stop();
                        log.warn("kafka listener '{}' is stopped!",c.getBeanName());
                    }
                })));
	}

}

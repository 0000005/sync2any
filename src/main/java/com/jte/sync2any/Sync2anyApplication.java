package com.jte.sync2any;

import com.jte.sync2any.conf.KafkaConfig;
import com.jte.sync2any.model.config.Sync2any;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author JerryYin
 */
@SpringBootApplication
@Configuration
@ConfigurationPropertiesScan
@EnableConfigurationProperties(Sync2any.class)
@Slf4j
public class Sync2anyApplication {

	public static void main(String[] args) {
		SpringApplication.run(Sync2anyApplication.class, args);
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

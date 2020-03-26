package com.jte.sync2es;

import com.jte.sync2es.model.config.Sync2es;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@Configuration
@ConfigurationPropertiesScan
@EnableConfigurationProperties(Sync2es.class)
public class Sync2esApplication {

	public static void main(String[] args) {
		SpringApplication.run(Sync2esApplication.class, args);
	}

}

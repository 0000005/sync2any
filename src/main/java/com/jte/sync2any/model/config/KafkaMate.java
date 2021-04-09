package com.jte.sync2any.model.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("kafka")
@Data
public class KafkaMate {
    private String address;
    private String username;
    private String password;
}

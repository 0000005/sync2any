package com.jte.sync2any.model.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
@Component
@ConfigurationProperties("target")
@Data
public class TargetDatasources {
    private List<Conn> datasources;
}

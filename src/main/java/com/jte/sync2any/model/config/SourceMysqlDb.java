package com.jte.sync2any.model.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
@Component
@ConfigurationProperties("source.mysql")
@Data
public class SourceMysqlDb {
    private List<Conn> datasources;
}

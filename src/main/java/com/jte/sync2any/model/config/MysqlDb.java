package com.jte.sync2any.model.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
@Component
@ConfigurationProperties("mysql")
@Data
public class MysqlDb {
    private List<Conn> datasources;
}

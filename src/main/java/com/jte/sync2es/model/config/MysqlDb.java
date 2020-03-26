package com.jte.sync2es.model.config;

import com.jte.sync2es.model.config.Conn;
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

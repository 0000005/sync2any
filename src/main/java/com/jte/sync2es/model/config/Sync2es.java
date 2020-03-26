package com.jte.sync2es.model.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
@Component
@ConfigurationProperties("sync2es")
@Data
public class Sync2es {
    /**
     *  which mysql database need to sync.
     */
    private List<SyncConfig> syncConfig;
}

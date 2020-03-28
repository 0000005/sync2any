package com.jte.sync2es.model.config;

import lombok.Data;

import java.util.List;

@Data
public class SyncConfig {
    private Mq mq;
    private String dbName;
    private String syncTables;
    private List<Rule> rules;
}

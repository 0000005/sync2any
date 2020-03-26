package com.jte.sync2es.model.config;

import lombok.Data;

import java.util.List;

@Data
public class SyncConfig {
    private Mq mq;
    private String dbId;
    private String syncTables;
    private List<Rule> rules;
}

package com.jte.sync2es.model.config;

import lombok.Data;

@Data
public class Rule {
    private String table;
    private String index;
    private String map;
    private String fieldFilter;
}

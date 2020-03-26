package com.jte.sync2es.model.config;

import lombok.Data;

@Data
public class Mq {
    private String topicName;
    private String topicGroup;
}

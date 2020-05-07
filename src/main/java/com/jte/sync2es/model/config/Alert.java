package com.jte.sync2es.model.config;

import lombok.Data;

@Data
public class Alert {
    private String secret;
    private String appId;
    private String delayTemplateId;
    private String idleTemplateId;
    private String errorTemplateId;
}

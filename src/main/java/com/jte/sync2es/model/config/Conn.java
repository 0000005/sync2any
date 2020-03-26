package com.jte.sync2es.model.config;

import lombok.Data;

@Data
public class Conn {
    private String id;
    private String url;
    private String username;
    private String password;
    private String driverClassName;
}

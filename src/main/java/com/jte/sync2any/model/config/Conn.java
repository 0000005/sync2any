package com.jte.sync2any.model.config;

import lombok.Data;

@Data
public class Conn {
    private String dbName;
    private String url;
    private String username;
    private String password;
    private String driverClassName;
}

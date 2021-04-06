package com.jte.sync2any.model.config;

import lombok.Data;

@Data
public class Conn {
    /**
     * 默认mysql
     */
    private String type="mysql";
    private String dbName;
    private String url;
    private String username;
    private String password;
}

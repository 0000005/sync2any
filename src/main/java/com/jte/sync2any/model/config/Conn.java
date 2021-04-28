package com.jte.sync2any.model.config;

import lombok.Data;

@Data
public class Conn {
    public static String DB_TYPE_ES="es";
    public static String DB_TYPE_MYSQL="mysql";
    /**
     * 默认mysql
     */
    private String type=DB_TYPE_MYSQL;
    private String dbId;
    private String dbName;
    private String url;
    private String username;
    private String password;
}

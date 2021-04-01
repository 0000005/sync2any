package com.jte.sync2any.model.mysql;

import lombok.Data;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Data
public class MyDatasource extends DriverManagerDataSource {
    private String dbName;
}

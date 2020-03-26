package com.jte.sync2es.model.mysql;

import lombok.Data;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Data
public class MyDatasource extends DriverManagerDataSource {
    private String id;
}

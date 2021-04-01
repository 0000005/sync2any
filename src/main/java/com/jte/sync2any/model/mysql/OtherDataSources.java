package com.jte.sync2any.model.mysql;

import lombok.Data;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@Data
public class OtherDataSources {
    private List<DataSource> dsList= new ArrayList<>();
}

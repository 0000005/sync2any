package com.jte.sync2es.config;

import com.jte.sync2es.Tester;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.Map;

public class MysqlDatasourceConfigTest extends Tester {

    @Autowired
    @Qualifier("allTemplate")
    Map<String,JdbcTemplate> allTemplate;

    @Test
    public void testDbConnection()
    {
        Assert.assertTrue(allTemplate.keySet().size()>0);
        for(String id:allTemplate.keySet())
        {
            JdbcTemplate jdbcTemplate=allTemplate.get(id);
            int n=jdbcTemplate.queryForObject("select 1",Integer.class);
            Assert.assertTrue(n==1);
        }
    }
}

package com.jte.sync2any.config;

import com.jte.sync2any.Tester;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

public class SourceMysqlDatasourceConfigTest extends Tester {

    @Autowired
    @Qualifier("allSourceTemplate")
    Map<String,JdbcTemplate> allSourceTemplate;

    @Test
    public void testDbConnection()
    {
        Assert.assertTrue(allSourceTemplate.keySet().size()>0);
        for(String id:allSourceTemplate.keySet())
        {
            JdbcTemplate jdbcTemplate=allSourceTemplate.get(id);
            int n=jdbcTemplate.queryForObject("select 1",Integer.class);
            Assert.assertTrue(n==1);
        }
    }
}

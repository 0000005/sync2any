package com.jte.sync2es.model.config;

import com.jte.sync2es.Tester;
import com.jte.sync2es.model.config.MysqlDb;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;
@Slf4j
public class MysqlDbTest extends Tester {

    @Resource
    MysqlDb mysqlDb;

    @Test
    public void testReadSyncConfig()
    {
        log.debug(mysqlDb.toString());
        Assert.assertNotNull(mysqlDb.getDatasources());
    }

}

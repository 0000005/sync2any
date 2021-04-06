package com.jte.sync2any.model.config;

import com.jte.sync2any.Tester;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;
@Slf4j
public class SourceMysqlDbTest extends Tester {

    @Resource
    SourceMysqlDb sourceMysqlDb;

    @Test
    public void testReadSyncConfig()
    {
        log.debug(sourceMysqlDb.toString());
        Assert.assertNotNull(sourceMysqlDb.getDatasources());
    }

}

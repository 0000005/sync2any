package com.jte.sync2es.model;

import com.jte.sync2es.Tester;
import com.jte.sync2es.model.config.Sync2es;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;
@Slf4j
public class Sync2esTest extends Tester {
    @Resource
    Sync2es sync2es;


    @Test
    public void testReadSyncConfig()
    {
        log.debug(sync2es.toString());
        Assert.assertNotNull(sync2es.getSyncConfig());
    }

}

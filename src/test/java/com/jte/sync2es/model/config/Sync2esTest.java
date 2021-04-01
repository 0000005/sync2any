package com.jte.sync2any.model.config;

import com.jte.sync2any.Tester;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;
@Slf4j
public class sync2anyTest extends Tester {
    @Resource
    sync2any sync2any;


    @Test
    public void testReadSyncConfig()
    {
        log.debug(sync2any.toString());
        Assert.assertNotNull(sync2any.getSyncConfigList());
    }

}

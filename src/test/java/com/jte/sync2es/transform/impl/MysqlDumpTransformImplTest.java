package com.jte.sync2es.transform.impl;

import com.jte.sync2es.Tester;
import com.jte.sync2es.conf.RuleConfigParser;
import com.jte.sync2es.model.mysql.TableMeta;
import com.jte.sync2es.transform.DumpTransform;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Resource;
import java.io.File;
import java.io.FileNotFoundException;

public class MysqlDumpTransformImplTest  extends Tester {
    @Resource
    DumpTransform dumpTransform;
    @Resource
    RuleConfigParser ruleParser;

    @Test
    public void transformUpdateTest() throws FileNotFoundException {
        TableMeta tableMeta=RuleConfigParser.RULES_MAP.getIfPresent("test$member");
        File file = new File(this.getClass().getResource("/test/dump.data.sql").getFile());
        MysqlDumpTransformImpl.FileEsRequest result= (MysqlDumpTransformImpl.FileEsRequest) dumpTransform.transform(file,tableMeta);
        while (result.hasNext())
        {
            result.next();
        }

    }

    @Before
    public void initRules()
    {
        ruleParser.initRules();
    }
}

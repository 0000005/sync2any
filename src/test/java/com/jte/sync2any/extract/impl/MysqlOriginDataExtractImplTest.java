package com.jte.sync2any.extract.impl;

import com.jte.sync2any.Tester;
import com.jte.sync2any.conf.RuleConfigParser;
import com.jte.sync2any.extract.OriginDataExtract;
import com.jte.sync2any.model.config.Sync2any;
import com.jte.sync2any.model.mysql.TableMeta;
import org.buildobjects.process.ProcBuilder;
import org.buildobjects.process.ProcResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Resource;
import java.sql.SQLException;

public class MysqlOriginDataExtractImplTest extends Tester {
    @Resource
    OriginDataExtract originDataExtract;

    @Resource
    RuleConfigParser ruleParser;

    @Resource
    Sync2any sync2any;

    @Before
    public void initRules()
    {
        ruleParser.initAllRules();
    }

    @Test
    public void dumpDataTest() throws SQLException, IllegalAccessException {
        TableMeta tableMeta=RuleConfigParser.RULES_MAP.getIfPresent("test$wzh");
        Assert.assertNotNull(originDataExtract.dumpData(tableMeta));
    }

    @Test
    public void commandTest()
    {
        ProcBuilder builder = new ProcBuilder(sync2any.getMysqldump());
        builder.withArg("--help");
        ProcResult result=builder.run();
        System.out.println(result.getOutputString());
    }

}

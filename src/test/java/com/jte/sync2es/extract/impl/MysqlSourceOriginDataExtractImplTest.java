package com.jte.sync2es.extract.impl;

import com.jte.sync2es.Tester;
import com.jte.sync2es.conf.RuleConfigParser;
import com.jte.sync2es.extract.SourceOriginDataExtract;
import com.jte.sync2es.model.config.Sync2es;
import com.jte.sync2es.model.mysql.TableMeta;
import org.buildobjects.process.ProcBuilder;
import org.buildobjects.process.ProcResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Resource;
import java.sql.SQLException;

public class MysqlSourceOriginDataExtractImplTest  extends Tester {
    @Resource
    SourceOriginDataExtract sourceOriginDataExtract;

    @Resource
    RuleConfigParser ruleParser;

    @Resource
    Sync2es sync2es;

    @Before
    public void initRules()
    {
        ruleParser.initRules();
    }

    @Test
    public void dumpDataTest() throws SQLException, IllegalAccessException {
        TableMeta tableMeta=RuleConfigParser.RULES_MAP.getIfPresent("test$wzh");
        Assert.assertNotNull(sourceOriginDataExtract.dumpData(tableMeta));
    }

    @Test
    public void commandTest()
    {
        ProcBuilder builder = new ProcBuilder(sync2es.getMysqldump());
        builder.withArg("--help");
        ProcResult result=builder.run();
        System.out.println(result.getOutputString());
    }

}

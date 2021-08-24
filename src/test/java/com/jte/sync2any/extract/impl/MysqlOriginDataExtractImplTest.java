package com.jte.sync2any.extract.impl;

import com.jte.sync2any.conf.RuleConfigParser;
import com.jte.sync2any.extract.OriginDataExtract;
import com.jte.sync2any.model.config.Sync2any;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.transform.impl.MysqlDumpTransformImpl;
import org.buildobjects.process.ProcBuilder;
import org.buildobjects.process.ProcResult;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class MysqlOriginDataExtractImplTest  {
    @Resource
    OriginDataExtract originDataExtract;

    @Resource
    RuleConfigParser ruleParser;

    @Resource
    Sync2any sync2any;

//    @Before
//    public void initRules()
//    {
//        ruleParser.initAllRules();
//    }

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

    @Test
    public void sqlTest() throws FileNotFoundException {
        TableMeta tableMeta=RuleConfigParser.RULES_MAP.getIfPresent("test$t_pms_member");
        File file = new File("C:\\Users\\JerryYin\\Desktop\\t_pms_room_log.error.sql");
        Iterator<List<CudRequest>> result=  new MysqlDumpTransformImpl().transform(file,tableMeta);
        while (result.hasNext())
        {
            List<CudRequest> r=result.next();
            System.out.println(r.size());
        }
    }

}

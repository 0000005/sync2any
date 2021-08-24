package com.jte.sync2any.transform.impl;

import com.jte.sync2any.Tester;
import com.jte.sync2any.conf.RuleConfigParser;
import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.transform.DumpTransform;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Resource;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;

public class MysqlDumpTransformImplTest  extends Tester {
    @Resource
    DumpTransform dumpTransform;
    @Resource
    RuleConfigParser ruleParser;

    @Test
    public void transformUpdateTest() throws FileNotFoundException {
        TableMeta tableMeta=RuleConfigParser.RULES_MAP.getIfPresent("test$t_pms_member");
        File file = new File(this.getClass().getResource("C:\\Users\\JerryYin\\Desktop\\t_pms_room_log.error.sql").getFile());
        Iterator<List<CudRequest>> result=  new MysqlDumpTransformImpl().transform(file,tableMeta);
        while (result.hasNext())
        {
            List<CudRequest> r=result.next();
            System.out.println(r.size());
        }
    }

    @Before
    public void initRules()
    {
        ruleParser.initAllRules();
    }
}

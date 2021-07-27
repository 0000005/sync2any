package com.jte.sync2any.extract.impl;

import cn.hutool.core.io.file.FileReader;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.jte.sync2any.conf.RuleConfigParser;
import com.jte.sync2any.extract.OriginDataExtract;
import com.jte.sync2any.model.config.Sync2any;
import com.jte.sync2any.model.mysql.TableMeta;
import org.buildobjects.process.ProcBuilder;
import org.buildobjects.process.ProcResult;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;
import java.sql.SQLException;
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
    public void sqlTest()
    {
        FileReader fileReader = new FileReader("C:\\Users\\JerryYin\\Desktop\\room_tail.sql");
        List<String> lines=fileReader.readLines();
        for(String line:lines){
            MySqlStatementParser parser = new MySqlStatementParser(line);
            SQLStatement statement = parser.parseStatement();
            MySqlInsertStatement insert = (MySqlInsertStatement)statement;
            System.out.println(insert.getTableName()+"-"+insert.getValuesList().size());
            if(insert.getValuesList().size() == 2062){
                System.out.println(line);
            }
        }
    }

}

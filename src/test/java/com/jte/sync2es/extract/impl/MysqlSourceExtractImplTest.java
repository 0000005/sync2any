package com.jte.sync2es.extract.impl;

import com.jte.sync2es.Tester;
import com.jte.sync2es.extract.SourceExtract;
import com.jte.sync2es.model.mysql.TableMeta;
import org.junit.Assert;
import org.junit.Test;
import org.junit.platform.commons.util.StringUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;

public class MysqlSourceExtractImplTest extends Tester {

    @Resource
    SourceExtract sourceExtract;

    @Test
    public void getTableMateTest(){
        TableMeta tableMeta=sourceExtract.getTableMate("test","wzh");
        Assert.assertTrue(Objects.nonNull(tableMeta));
        Assert.assertTrue(tableMeta.getAllColumnMap().keySet().size()>0);
        Assert.assertTrue(StringUtils.isNotBlank(tableMeta.getTableName()));
    }

    @Test
    public void getAllTableNameTest()
    {
        List<String> tableNames=sourceExtract.getAllTableName("jte253");
        System.out.println(tableNames);
        Assert.assertTrue(!tableNames.isEmpty());
    }

}

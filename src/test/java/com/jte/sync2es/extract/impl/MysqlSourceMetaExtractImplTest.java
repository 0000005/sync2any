package com.jte.sync2any.extract.impl;

import com.jte.sync2any.Tester;
import com.jte.sync2any.extract.SourceMetaExtract;
import com.jte.sync2any.model.mysql.TableMeta;
import org.junit.Assert;
import org.junit.Test;
import org.junit.platform.commons.util.StringUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;

public class MysqlSourceMetaExtractImplTest extends Tester {

    @Resource
    SourceMetaExtract sourceMetaExtract;

    @Test
    public void getTableMateTest(){
        TableMeta tableMeta= sourceMetaExtract.getTableMate("test","wzh");
        Assert.assertTrue(Objects.nonNull(tableMeta));
        Assert.assertTrue(tableMeta.getAllColumnMap().keySet().size()>0);
        Assert.assertTrue(StringUtils.isNotBlank(tableMeta.getTableName()));
    }

    @Test
    public void getAllTableNameTest()
    {
        List<String> tableNames= sourceMetaExtract.getAllTableName("test");
        System.out.println(tableNames);
        Assert.assertTrue(!tableNames.isEmpty());
    }

    @Test
    public void getDataCountTest() {
        Long count=sourceMetaExtract.getDataCount("test","wzh");
        Assert.assertTrue(count>0);
    }

}

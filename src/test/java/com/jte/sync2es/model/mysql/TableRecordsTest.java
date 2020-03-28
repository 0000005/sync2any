package com.jte.sync2es.model.mysql;

import com.jte.sync2es.Tester;
import com.jte.sync2es.conf.RuleConfigParser;
import com.jte.sync2es.exception.IllegalDataStructureException;
import com.jte.sync2es.model.mq.TcMqMessage;
import com.jte.sync2es.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.platform.commons.util.StringUtils;

import javax.annotation.Resource;

@Slf4j
public class TableRecordsTest extends Tester {
    @Resource
    RuleConfigParser ruleParser;

    public final String updateMsg="{\"prefix\":\"dRO1B\",\"filename\":\"/data/tdengine/log/4364/dblogs/bin/binlog.000074\",\"logtype\":\"mysqlbinlog\",\"eventtype\":31,\"eventtypestr\":\"update\",\"db\":\"test\",\"table\":\"wzh\",\"localip\":\"\",\"localport\":0,\"begintime\":1585202114,\"gtid\":\"d3df5e98-0a88-11ea-bf79-246e965b5b98:15157638\",\"serverid\":\"3111177740\",\"event_index\":\"4\",\"gtid_commitid\":\"\",\"gtid_flag2\":\"0\",\"where\":[\"1\",\"'xxxx'\",\"'b'\",\"'2020-03-18 18:01:07'\"],\"field\":[\"2\",\"'xxxx'\",\"'b'\",\"'2020-03-18 18:01:07'\"],\"sub_event_index\":\"1\",\"sequence_num\":\"276873\",\"orgoffset\":59664450}";

    public final String deleteMsg="{\"prefix\":\"dRO1B\",\"filename\":\"/data/tdengine/log/4364/dblogs/bin/binlog.000074\",\"logtype\":\"mysqlbinlog\",\"eventtype\":32,\"eventtypestr\":\"delete\",\"db\":\"test\",\"table\":\"wzh\",\"localip\":\"\",\"localport\":0,\"begintime\":1585189964,\"gtid\":\"d3df5e98-0a88-11ea-bf79-246e965b5b98:15141012\",\"serverid\":\"3111177740\",\"event_index\":\"4\",\"gtid_commitid\":\"\",\"gtid_flag2\":\"0\",\"where\":[\"8\",\"'asdf'\",\"'v'\",\"'2020-03-26 10:31:42'\"],\"field\":[],\"sub_event_index\":\"1\",\"sequence_num\":\"226946\",\"orgoffset\":59614523}";

    public final String insertMsg="{\"prefix\":\"dRO1B\",\"filename\":\"/data/tdengine/log/4364/dblogs/bin/binlog.000074\",\"logtype\":\"mysqlbinlog\",\"eventtype\":30,\"eventtypestr\":\"insert\",\"db\":\"test\",\"table\":\"wzh\",\"localip\":\"\",\"localport\":0,\"begintime\":1585189905,\"gtid\":\"d3df5e98-0a88-11ea-bf79-246e965b5b98:15140930\",\"serverid\":\"3111177740\",\"event_index\":\"4\",\"gtid_commitid\":\"\",\"gtid_flag2\":\"0\",\"where\":[],\"field\":[\"8\",\"'asdf'\",\"'v'\",\"'2020-03-26 10:31:42'\"],\"sub_event_index\":\"1\",\"sequence_num\":\"226698\",\"orgoffset\":59614275}\n";



    @Test
    public void testBuildRecordsForUpdate() throws IllegalDataStructureException {
        TcMqMessage message =JsonUtil.jsonToPojo(updateMsg,TcMqMessage.class);
        TableMeta tableMeta=RuleConfigParser.RULES_MAP.getIfPresent("test$wzh");
        TableRecords tableRecords=TableRecords.buildRecords(tableMeta,message);
        System.out.println("testBuildRecordsForDelete:"+JsonUtil.objectToJson(tableRecords));
        Assert.assertTrue(StringUtils.isNotBlank(tableRecords.getTableName()));
        Assert.assertEquals(tableRecords.getFieldRows().size(),1);
        Assert.assertEquals(tableRecords.getWhereRows().size(),1);
        Assert.assertNotNull(tableRecords.getMqMessage());
        Assert.assertNotNull(tableRecords.getTableMeta());
        Assert.assertEquals(tableRecords.getFieldRows().get(0).getFields().get(0).getName(),"id");
        Assert.assertEquals(tableRecords.getFieldRows().get(0).getFields().get(0).getKeyType(),KeyType.PRIMARY_KEY);
        Assert.assertEquals(tableRecords.getWhereRows().get(0).getFields().get(0).getName(),"id");
        Assert.assertEquals(tableRecords.getWhereRows().get(0).getFields().get(0).getKeyType(),KeyType.PRIMARY_KEY);
    }


    @Test
    public void testBuildRecordsForDelete() throws IllegalDataStructureException {
        TcMqMessage message =JsonUtil.jsonToPojo(deleteMsg,TcMqMessage.class);
        TableMeta tableMeta=RuleConfigParser.RULES_MAP.getIfPresent("test$wzh");
        TableRecords tableRecords=TableRecords.buildRecords(tableMeta,message);
        System.out.println("testBuildRecordsForUpdate:"+JsonUtil.objectToJson(tableRecords));
        Assert.assertTrue(StringUtils.isNotBlank(tableRecords.getTableName()));
        Assert.assertEquals(0,tableRecords.getFieldRows().size());
        Assert.assertEquals(1,tableRecords.getWhereRows().size());
        Assert.assertNotNull(tableRecords.getMqMessage());
        Assert.assertNotNull(tableRecords.getTableMeta());
        Assert.assertEquals("id",tableRecords.getWhereRows().get(0).getFields().get(0).getName());
        Assert.assertEquals(KeyType.PRIMARY_KEY,tableRecords.getWhereRows().get(0).getFields().get(0).getKeyType());
    }


    @Test
    public void testBuildRecordsForInsert() throws IllegalDataStructureException {
        TcMqMessage message =JsonUtil.jsonToPojo(insertMsg,TcMqMessage.class);
        TableMeta tableMeta=RuleConfigParser.RULES_MAP.getIfPresent("test$wzh");
        TableRecords tableRecords=TableRecords.buildRecords(tableMeta,message);
        System.out.println("testBuildRecordsForInsert:"+JsonUtil.objectToJson(tableRecords));
        Assert.assertTrue(StringUtils.isNotBlank(tableRecords.getTableName()));
        Assert.assertEquals(tableRecords.getFieldRows().size(),1);
        Assert.assertEquals(tableRecords.getWhereRows().size(),0);
        Assert.assertNotNull(tableRecords.getMqMessage());
        Assert.assertNotNull(tableRecords.getTableMeta());
        Assert.assertEquals(tableRecords.getFieldRows().get(0).getFields().get(0).getName(),"id");
        Assert.assertEquals(tableRecords.getFieldRows().get(0).getFields().get(0).getKeyType(),KeyType.PRIMARY_KEY);
    }

    @Before
    public void initRules()
    {
        ruleParser.initRules();
    }

}

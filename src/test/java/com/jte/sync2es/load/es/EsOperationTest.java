package com.jte.sync2es.load.es;

import com.jte.sync2es.Tester;
import com.jte.sync2es.conf.RuleConfigParser;
import com.jte.sync2es.model.es.EsRequest;
import com.jte.sync2es.model.mq.TcMqMessage;
import com.jte.sync2es.model.mysql.TableMeta;
import com.jte.sync2es.model.mysql.TableRecords;
import com.jte.sync2es.transform.RecordsTransform;
import com.jte.sync2es.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Resource;
import java.io.IOException;

@Slf4j
public class EsOperationTest extends Tester {

    public final String updateMsg="{\"prefix\":\"Xpyba\",\"filename\":\"/data/tdengine/log/4364/dblogs/bin/binlog.000081\",\"logtype\":\"mysqlbinlog\",\"eventtype\":31,\"eventtypestr\":\"update\",\"db\":\"test\",\"table\":\"wzh\",\"localip\":\"\",\"localport\":0,\"begintime\":1586502642,\"gtid\":\"d3df5e98-0a88-11ea-bf79-246e965b5b98:17022935\",\"serverid\":\"3111177740\",\"event_index\":\"4\",\"gtid_commitid\":\"\",\"gtid_flag2\":\"0\",\"where\":[\"13\",\"'尹文俊'\",\"'计算机'\",\"'2020-04-10 15:09:46'\"],\"field\":[\"13\",\"'尹文俊和冯豆'\",\"'计算机、旅游'\",\"'2020-04-01 15:09:46'\"],\"sub_event_index\":\"1\",\"sequence_num\":\"15267\",\"orgoffset\":65664364}\n";

    public final String deleteMsg="{\"prefix\":\"Xpyba\",\"filename\":\"/data/tdengine/log/4364/dblogs/bin/binlog.000081\",\"logtype\":\"mysqlbinlog\",\"eventtype\":32,\"eventtypestr\":\"delete\",\"db\":\"test\",\"table\":\"wzh\",\"localip\":\"\",\"localport\":0,\"begintime\":1586502672,\"gtid\":\"d3df5e98-0a88-11ea-bf79-246e965b5b98:17022976\",\"serverid\":\"3111177740\",\"event_index\":\"4\",\"gtid_commitid\":\"\",\"gtid_flag2\":\"0\",\"where\":[\"13\",\"'尹文俊和冯豆'\",\"'计算机、旅游'\",\"'2020-04-01 15:09:46'\"],\"field\":[],\"sub_event_index\":\"1\",\"sequence_num\":\"15390\",\"orgoffset\":65664487}";

    public final String insertMsg="{\"prefix\":\"Xpyba\",\"filename\":\"/data/tdengine/log/4364/dblogs/bin/binlog.000081\",\"logtype\":\"mysqlbinlog\",\"eventtype\":30,\"eventtypestr\":\"insert\",\"db\":\"test\",\"table\":\"wzh\",\"localip\":\"\",\"localport\":0,\"begintime\":1586502595,\"gtid\":\"d3df5e98-0a88-11ea-bf79-246e965b5b98:17022869\",\"serverid\":\"3111177740\",\"event_index\":\"4\",\"gtid_commitid\":\"\",\"gtid_flag2\":\"0\",\"where\":[],\"field\":[\"13\",\"'尹文俊'\",\"'计算机'\",\"'2020-04-10 15:09:46'\"],\"sub_event_index\":\"1\",\"sequence_num\":\"15069\",\"orgoffset\":65664166}";

    @Resource
    EsLoadServiceImpl esLoadService;

    @Resource
    private RecordsTransform transform;

    @Resource
    RuleConfigParser ruleParser;

    private EsRequest insertRequest;
    private EsRequest updateRequest;
    private EsRequest deleteRequest;

    @Before
    public void init(){
        ruleParser.initRules();

        TableMeta tableMeta=RuleConfigParser.RULES_MAP.getIfPresent("test$wzh");

        TcMqMessage insertMessage =JsonUtil.jsonToPojo(insertMsg,TcMqMessage.class);
        TableRecords insertTableRecords=TableRecords.buildRecords(tableMeta,insertMessage);
        insertRequest =transform.transform(insertTableRecords);

        TcMqMessage updateMessage =JsonUtil.jsonToPojo(updateMsg,TcMqMessage.class);
        TableRecords updateTableRecords=TableRecords.buildRecords(tableMeta,updateMessage);
        updateRequest =transform.transform(updateTableRecords);

        TcMqMessage deleteMessage =JsonUtil.jsonToPojo(deleteMsg,TcMqMessage.class);
        TableRecords deleteTableRecords=TableRecords.buildRecords(tableMeta,deleteMessage);
        deleteRequest =transform.transform(deleteTableRecords);

    }

    @Test
    public void checkAndCreateStorageTest() throws IOException {
        esLoadService.checkAndCreateStorage(insertRequest);
    }

    @Test
    public void addDataTest() throws IOException {
        esLoadService.addData(insertRequest);
    }
    @Test
    public void updateDataTest() throws IOException {
        esLoadService.updateData(updateRequest);
    }

    @Test
    public void deleteDataTest() throws IOException {
        esLoadService.deleteData(deleteRequest);
    }


    @Test
    public void generateMappingJsonTest()
    {
        System.out.println(esLoadService.generateMappingJson(insertRequest));
    }



}

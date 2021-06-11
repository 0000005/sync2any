package com.jte.sync2any.extract.impl;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author JerryYin
 * @since 2021-06-04 15:11
 */
public class CkMetaExtractImplTest {
    @Test
    public void getTableNameFromEngineFullTest(){
        String answer = "t_pms_account_local";
        CkMetaExtractImpl ck =new CkMetaExtractImpl();
        Assert.assertEquals(answer,ck.getTableNameFromEngineFull("Distributed('c_2_2', 'ck_jte', 't_pms_account_local' , cityHash64(group_code))"));
        Assert.assertEquals(answer,ck.getTableNameFromEngineFull("Distributed('c_2_2', 'ck_jte', 't_pms_account_local')"));
        Assert.assertEquals(answer,ck.getTableNameFromEngineFull("Distributed(c_2_2, ck_jte, t_pms_account_local , cityHash64(group_code))"));
        Assert.assertEquals(answer,ck.getTableNameFromEngineFull("Distributed(c_2_2, ck_jte, t_pms_account_local, group_code)"));
    }
}

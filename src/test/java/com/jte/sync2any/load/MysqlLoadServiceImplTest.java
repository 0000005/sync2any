package com.jte.sync2any.load;

import com.jte.sync2any.model.es.CudRequest;
import com.jte.sync2any.model.mysql.ColumnMeta;
import com.jte.sync2any.model.mysql.Field;
import com.jte.sync2any.model.mysql.TableMeta;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import javax.annotation.Resource;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author JerryYin
 * @since 2021-04-07 11:23
 */
@Slf4j
public class MysqlLoadServiceImplTest  {

    @Resource
    MysqlLoadServiceImpl mysqlLoadService;

    List<String> pkNameList = Arrays.asList("id","group_code");

    @Test
    public void buildWhereConditionSqlByPksTest(){

        TableMeta tableMeta =mock(TableMeta.class);
        when(tableMeta.getPrimaryKeyOnlyName()).thenReturn(pkNameList);
        String sql =new MysqlLoadServiceImpl().buildWhereConditionSqlByPks(tableMeta);
        assertEquals("id = ?  and group_code = ? ",sql);
    }

    @Test
    public void fillDeleteParamTest(){
        Field idField = new Field();
        idField.setValue(1);
        Field groupCodeField = new Field();
        groupCodeField.setValue("66666");
        Map<String, Field>  pkValueMap = new HashMap<String, Field> () {{
            put("id", idField);
            put("group_code", groupCodeField);
        }};

        TableMeta tableMeta =mock(TableMeta.class);
        when(tableMeta.getPrimaryKeyOnlyName()).thenReturn(pkNameList);
        CudRequest cudRequest=mock(CudRequest.class);
        when(cudRequest.getTableMeta()).thenReturn(tableMeta);
        when(cudRequest.getPkValueMap()).thenReturn(pkValueMap);
        Object[] params =new MysqlLoadServiceImpl().fillDeleteParam(cudRequest);
        assertArrayEquals(new Object[]{1,"66666"},params);
    }

    @Test
    public void buildInsertSqlByPksTest(){
        ColumnMeta idColumn = new ColumnMeta();
        idColumn.setColumnName("id");
        ColumnMeta groupCodeColumn = new ColumnMeta();
        groupCodeColumn.setColumnName("groupCode");
        List<ColumnMeta> allColumnList =new ArrayList<>();
        allColumnList.add(idColumn);
        allColumnList.add(groupCodeColumn);

        TableMeta tableMeta =mock(TableMeta.class);
        when(tableMeta.getAllColumnList()).thenReturn(allColumnList);
        when(tableMeta.getTableName()).thenReturn("test");
        String sql=new MysqlLoadServiceImpl().buildInsertSqlByPks(tableMeta);
        assertEquals("INSERT INTO test (id, groupCode) VALUES (?, ?)",sql);
    }


    @Test
    public void fillInsertParamTest(){
        ColumnMeta idColumn = new ColumnMeta();
        idColumn.setColumnName("id");
        ColumnMeta groupCodeColumn = new ColumnMeta();
        groupCodeColumn.setColumnName("group_code");
        List<ColumnMeta> allColumnList =new ArrayList<>();
        allColumnList.add(idColumn);
        allColumnList.add(groupCodeColumn);
        Map<String, Object>  parameters = new HashMap<String, Object> () {{
            put("id", 1);
            put("group_code", "66666");
        }};

        TableMeta tableMeta =mock(TableMeta.class);
        when(tableMeta.getAllColumnList()).thenReturn(allColumnList);
        CudRequest cudRequest=mock(CudRequest.class);
        when(cudRequest.getTableMeta()).thenReturn(tableMeta);
        when(cudRequest.getParameters()).thenReturn(parameters);
        Object[] params =new MysqlLoadServiceImpl().fillInsertParam(cudRequest);
        assertArrayEquals(new Object[]{1,"66666"},params);
    }

}

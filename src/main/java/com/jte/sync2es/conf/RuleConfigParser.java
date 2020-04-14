package com.jte.sync2es.conf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jte.sync2es.exception.ShouldNeverHappenException;
import com.jte.sync2es.extract.SourceMetaExtract;
import com.jte.sync2es.model.config.MysqlDb;
import com.jte.sync2es.model.config.Rule;
import com.jte.sync2es.model.config.Sync2es;
import com.jte.sync2es.model.config.SyncConfig;
import com.jte.sync2es.model.es.EsDateType;
import com.jte.sync2es.model.mysql.ColumnMeta;
import com.jte.sync2es.model.mysql.TableMeta;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 规则解析器：
 * 1、哪些表需要被传输
 * 2、表的字段名称和字段类型的映射
 * 3、索引名称的映射
 * 4、过滤器的规则
 */

@Configuration
public class RuleConfigParser {


    /**
     * key: dbName$tableName
     * value: TableMeta
     */
    public static final Cache<String, TableMeta> RULES_MAP = CacheBuilder.newBuilder().build();

    @Resource
    SourceMetaExtract sourceMetaExtract;
    @Resource
    Sync2es sync2es;
    @Resource
    MysqlDb mysqlDb;

    public void initRules() {
        mysqlDb.getDatasources().forEach(db->{
            //获取所有的表名
            List<String> tableNameList= sourceMetaExtract.getAllTableName(db.getDbName());
            //找到当前数据库的所有规则
            List<SyncConfig> currSyncConfigList=sync2es.getSyncConfig().stream()
                    .filter(s->db.getDbName().equalsIgnoreCase(s.getDbName()))
                    .collect(Collectors.toList());
            for(int r=0;r<currSyncConfigList.size();r++)
            {
                SyncConfig config=currSyncConfigList.get(r);
                //查看表名是否匹配
                String [] syncTableArray =config.getSyncTables().split(",");
                for(String syncTableName :syncTableArray)
                {
                    List<String> matchTableName=tableNameList.stream()
                            .filter(rt->Pattern.matches(syncTableName, rt))
                            .collect(Collectors.toList());
                    //接下来解析rule
                    for(String realTableName:matchTableName)
                    {
                        String key=config.getDbName()+"$"+realTableName;
                        //寻找到了匹配的表
                        TableMeta tableMeta=RULES_MAP.getIfPresent(key);
                        if(Objects.nonNull(tableMeta) )
                        {
                            continue;
                        }
                        tableMeta.setTopicName(config.getMq().getTopicName());
                        tableMeta.setTopicGroup(config.getMq().getTopicGroup());
                        //该表还未解析规则，寻找规则
                        Rule rule=config.getRules().stream()
                                .filter(tr -> Pattern.matches(tr.getTable(),realTableName))
                                .findFirst().orElse(new Rule());
                        tableMeta = sourceMetaExtract.getTableMate(db.getDbName(),realTableName);

                        //填充匹配规则
                        parseColumnMeta(tableMeta,rule);
                        RULES_MAP.put(key,tableMeta);
                    }
                }
            }
        });

    }

    private void checkEsDataType(String dataType){
        if(Objects.isNull(EsDateType.getDataType(dataType)))
        {
            throw new ShouldNeverHappenException("not support this data type for now. data type:"+dataType);
        }
    }

    private ColumnMeta mapDataTypeOfEs(ColumnMeta columnMeta){
        switch (columnMeta.getDataType()) {
            // BOOLEAN -->boolean
            case Types.BOOLEAN:
                columnMeta.setEsDataType(EsDateType.BOOLEAN.getDataType());
                break;
            //  INTEGER TINYINT -->integer
            case Types.TINYINT:
                columnMeta.setEsDataType(EsDateType.INTEGER.getDataType());
                break;
            case Types.INTEGER:
                columnMeta.setEsDataType(EsDateType.INTEGER.getDataType());
                break;
            // BIGINT NUMERIC-->long
            case Types.NUMERIC:
                columnMeta.setEsDataType(EsDateType.LONG.getDataType());
                break;
            case Types.BIGINT:
                columnMeta.setEsDataType(EsDateType.LONG.getDataType());
                break;
            // DECIMAL DOUBLE-->double
            case Types.DECIMAL:
                columnMeta.setEsDataType(EsDateType.DOUBLE.getDataType());
                break;
            case Types.DOUBLE:
                columnMeta.setEsDataType(EsDateType.DOUBLE.getDataType());
                break;
            // FLOAT -->float
            case Types.FLOAT:
                columnMeta.setEsDataType(EsDateType.FLOAT.getDataType());
                break;
            // CHAR NCHAR VARCHAR NCLOB CLOB-->keyword text
            case Types.CHAR:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
                break;
            case Types.NCHAR:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
                break;
            case Types.VARCHAR:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
                break;
            case Types.NCLOB:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
                break;
            case Types.CLOB:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
                break;
            // DATE >date
            case Types.DATE:
                columnMeta.setEsDataType(EsDateType.DATA.getDataType());
                break;
            // datetime >date
            case Types.TIMESTAMP:
                columnMeta.setEsDataType(EsDateType.DATA.getDataType());
                break;
            // other --> text
            default:
                columnMeta.setEsDataType(EsDateType.TEXT.getDataType());
        }
        return columnMeta;
    }

    private TableMeta parseColumnMeta(TableMeta tableMeta,Rule rule){

        ObjectMapper jsonMapper = new ObjectMapper();
        //计算规则
        if(StringUtils.isNotBlank(rule.getIndex()))
        {
            tableMeta.setEsIndexName(rule.getIndex());
        }
        else
        {
            //默认index命名规则：dbName-tableName
            tableMeta.setEsIndexName(tableMeta.getDbName().toLowerCase()+"-"+tableMeta.getTableName().toLowerCase());
        }
        if(StringUtils.isNotBlank(rule.getFieldFilter()))
        {
            List<String>  includeFields= Arrays.asList(rule.getFieldFilter().toLowerCase().split(","));
            tableMeta.getAllColumnList()
                    .forEach(t->t.setInclude(includeFields.contains(t.getColumnName().toLowerCase())));
            List<String> pkColumnNameList=tableMeta.getPrimaryKeyOnlyName();
            boolean isContainPk=tableMeta.getAllColumnList().stream()
                    .filter(c->pkColumnNameList.contains(c.getColumnName().toLowerCase()))
                    .count()>0;
            if(!isContainPk)
            {
                throw new ShouldNeverHappenException("fields filter must contain pk column!");
            }
        }
        //计算字段
        if(StringUtils.isNotBlank(rule.getMap()))
        {
            Map<String, String> columnMap=null;
            try {
                columnMap=jsonMapper.readValue(rule.getMap(),Map.class);
            } catch (JsonProcessingException e) {
                throw new ShouldNeverHappenException("parse rule of map is failed, please make sure that map is a valid json。map:"+rule.getMap());
            }
            if(Objects.isNull(columnMap))
            {
                throw new ShouldNeverHappenException("parse result is null, map:"+rule.getMap());
            }
            for(String columnName:tableMeta.getAllColumnMap().keySet())
            {
                ColumnMeta columnMeta=tableMeta.getAllColumnMap().get(columnName);
                String mapValue=columnMap.get(columnName);
                if(StringUtils.isBlank(mapValue))
                {
                    //未配置规则，统一化为小写
                    columnMeta.setEsColumnName(columnName.toLowerCase());
                    mapDataTypeOfEs(columnMeta);
                    continue;
                }
                //有具体映射规则
                if(mapValue.contains(","))
                {
                    //有类型规则  [0] 映射的字段名，[1] 映射的字段类型
                    String [] mapValueArray=mapValue.split(",");
                    if(StringUtils.isNotBlank(mapValueArray[0]))
                    {
                        columnMeta.setEsColumnName(mapValueArray[0]);
                    }
                    else
                    {
                        columnMeta.setEsColumnName(columnName.toLowerCase());
                    }
                    if(StringUtils.isNotBlank(mapValueArray[1]))
                    {
                        checkEsDataType(mapValueArray[1]);
                        columnMeta.setEsDataType(mapValueArray[1]);
                    }
                    else
                    {
                        mapDataTypeOfEs(columnMeta);
                    }
                }
                else
                {
                    //仅有名称映射规则
                    columnMeta.setEsColumnName(mapValue);
                    mapDataTypeOfEs(columnMeta);
                }
            }
        }
        else
        {
            for(String columnName:tableMeta.getAllColumnMap().keySet())
            {
                ColumnMeta columnMeta=tableMeta.getAllColumnMap().get(columnName);
                //未配置规则，统一化为小写
                columnMeta.setEsColumnName(columnName.toLowerCase());
                mapDataTypeOfEs(columnMeta);
            }
        }
        return tableMeta;
    }
}

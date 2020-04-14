package com.jte.sync2es.extract.impl;

import com.jte.sync2es.exception.ShouldNeverHappenException;
import com.jte.sync2es.extract.SourceOriginDataExtract;
import com.jte.sync2es.model.config.Conn;
import com.jte.sync2es.model.config.MysqlDb;
import com.jte.sync2es.model.mysql.TableMeta;
import com.jte.sync2es.util.DbUtils;
import org.buildobjects.process.ProcBuilder;
import org.buildobjects.process.ProcResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Resource;
import java.io.File;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * 从mysql中提取原始数据
 */
public class MysqlSourceOriginDataExtractImpl implements SourceOriginDataExtract {


    @Autowired
    @Qualifier("allTemplate")
    Map<String,JdbcTemplate> allTemplate;


    @Resource
    MysqlDb mysqlDb;

    private final String dumpCommand="mysqldump -h#{host} -P${port} -u#{username} -p#{password} -t -c --compact --single-transaction --databases #{dbName} --tables #{tableName} > #{filePathName}";

    /**
     * example: mysqldump -h192.168.10.203 -uroot -pxyz11111111 -t -c --compact --single-transaction --databases jte_pms_member > /tmp/member.data.sql
     * @param tableMeta
     * @return
     */
    @Override
    public File dumpData(TableMeta tableMeta) throws SQLException, IllegalAccessException {
        Conn dbConfig= mysqlDb.getDatasources().stream()
                .filter(d->d.getDbName().equals(tableMeta.getDbName()))
                .findFirst().orElse(null);
        if(Objects.isNull(dbConfig))
        {
            throw new ShouldNeverHappenException("db config is not found:{}"+tableMeta.getDbName());
        }

        JdbcTemplate jdbcTemplate = allTemplate.get(tableMeta.getDbName());
        String dbUrl=jdbcTemplate.getDataSource().getConnection().getMetaData().getURL();
        Map<String,String> dbParam=DbUtils.getParamFromUrl(dbUrl);
        String filePath="/tmp/"+tableMeta.getDbName()+"_"+tableMeta.getTableName()+"_"+new Random().nextInt(99999)+".data.sql";
        String execCommand = new String(dumpCommand);
        execCommand=execCommand.replace("#{host}",dbParam.get("host"));
        execCommand=execCommand.replace("#{port}",dbParam.get("port"));
        execCommand=execCommand.replace("#{username}",dbConfig.getUsername());
        execCommand=execCommand.replace("#{password}",dbConfig.getPassword());
        execCommand=execCommand.replace("#{dbName}",tableMeta.getDbName());
        execCommand=execCommand.replace("#{tableName}",tableMeta.getTableName());
        execCommand=execCommand.replace("#{filePathName}",filePath);
        ProcBuilder builder = new ProcBuilder(execCommand);
        ProcResult result=builder.run();

        if(result.getExitValue()!=0)
        {
            throw new IllegalAccessException("mysql dump fail! exitValue:"+result.getExitValue()+" output:"+result.getOutputString());
        }

        File sqlFile= new File(filePath);
        if(!sqlFile.exists())
        {
            throw new ShouldNeverHappenException("mysqldump error,sql file is not exists!");
        }

        return sqlFile;
    }
}

package com.jte.sync2es.extract.impl;

import com.jte.sync2es.exception.ShouldNeverHappenException;
import com.jte.sync2es.extract.SourceOriginDataExtract;
import com.jte.sync2es.model.config.Conn;
import com.jte.sync2es.model.config.MysqlDb;
import com.jte.sync2es.model.config.Sync2es;
import com.jte.sync2es.model.mysql.TableMeta;
import com.jte.sync2es.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.buildobjects.process.ProcBuilder;
import org.buildobjects.process.ProcResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * 从mysql中提取原始数据
 */
@Service
@Slf4j
public class MysqlSourceOriginDataExtractImpl implements SourceOriginDataExtract {


    @Autowired
    @Qualifier("allTemplate")
    Map<String,JdbcTemplate> allTemplate;

    @Resource
    MysqlDb mysqlDb;

    @Resource
    Sync2es sync2es;

    /**
     *
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
        String filePath=System.getProperty("java.io.tmpdir")+File.separator+tableMeta.getDbName()+"_"+tableMeta.getTableName()+"_"+new Random().nextInt(99999)+".data.sql";
        File sqlFile= new File(filePath);

        ProcBuilder builder = new ProcBuilder(sync2es.getMysqldump());
        builder.withArg("-h"+dbParam.get("host"));
        builder.withArg("-P"+dbParam.get("port"));
        builder.withArg("-u"+dbConfig.getUsername());
        builder.withArg("-p"+dbConfig.getPassword());
        builder.withArg("-t");
        builder.withArg("-c");
        builder.withArg("--compact");
        builder.withArg("--single-transaction");
        builder.withArgs("--databases",tableMeta.getDbName());
        builder.withArgs("--tables",tableMeta.getTableName());
        builder.withOutputConsumer(stream -> FileUtils.copyToFile(stream,sqlFile));
        //30分钟超时
        builder.withTimeoutMillis(1000*60*30);
        ProcResult result=builder.run();


        if(result.getExitValue()!=0)
        {
            throw new IllegalAccessException("mysql dump fail! exitValue:"+result.getExitValue()+" output:"+result.getOutputString());
        }


        if(!sqlFile.exists())
        {
            throw new ShouldNeverHappenException("mysqldump error,sql file is not exists!");
        }

        return sqlFile;
    }
}

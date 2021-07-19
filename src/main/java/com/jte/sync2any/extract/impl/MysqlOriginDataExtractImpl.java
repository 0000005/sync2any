package com.jte.sync2any.extract.impl;

import cn.hutool.db.DbUtil;
import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.extract.OriginDataExtract;
import com.jte.sync2any.model.config.Conn;
import com.jte.sync2any.model.config.SourceMysqlDb;
import com.jte.sync2any.model.config.Sync2any;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.DbUtils;
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
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

/**
 * 从mysql中提取原始数据
 */
@Service
@Slf4j
public class MysqlOriginDataExtractImpl implements OriginDataExtract {


    @Autowired
    @Qualifier("allSourceTemplate")
    Map<String,JdbcTemplate> allSourceTemplate;

    @Resource
    SourceMysqlDb sourceMysqlDb;

    @Resource
    Sync2any sync2any;

    /**
     *
     * example: mysqldump -h192.168.10.203 -uroot -pxyz11111111 -t -c --compact --single-transaction --databases jte_pms_member > /tmp/member.data.sql
     * @param tableMeta
     * @return
     */
    @Override
    public File dumpData(TableMeta tableMeta) throws SQLException, IllegalAccessException {
        Connection dbConn = null;
        try{

            Conn dbConfig= sourceMysqlDb.getDatasources().stream()
                    .filter(d->d.getDbName().equals(tableMeta.getDbName()))
                    .findFirst().orElse(null);
            if(Objects.isNull(dbConfig))
            {
                throw new ShouldNeverHappenException("db config is not found:{}"+tableMeta.getDbName());
            }

            JdbcTemplate jdbcTemplate = allSourceTemplate.get(tableMeta.getSourceDbId());
            dbConn =jdbcTemplate.getDataSource().getConnection();
            String dbUrl=dbConn.getMetaData().getURL();
            Map<String,String> dbParam=DbUtils.getParamFromUrl(dbUrl);
            String filePath=System.getProperty("java.io.tmpdir")+File.separator+tableMeta.getDbName()+"_"+tableMeta.getTableName()+"_"+new SecureRandom().nextInt(99999)+".data.sql";
            File sqlFile= new File(filePath);
            log.info("正在dump数据，表：{}，sql文件：{}",tableMeta.getTableName(),filePath);
            ProcBuilder builder = new ProcBuilder(sync2any.getMysqldump());
            builder.withArg("-h"+dbParam.get("host"));
            builder.withArg("-P"+dbParam.get("port"));
            builder.withArg("-u"+dbConfig.getUsername());
            builder.withArg("-p"+dbConfig.getPassword());
            builder.withArg("-t");
            builder.withArg("-c");
            builder.withArg("--compact");
            builder.withArg("--single-transaction");
            builder.withArg("--skip-tz-utc");
            builder.withArgs("--databases",tableMeta.getDbName());
            builder.withArgs("--tables",tableMeta.getTableName());
            builder.withOutputConsumer(stream -> FileUtils.copyToFile(stream,sqlFile));
            //30分钟超时
            builder.withTimeoutMillis(1000*60*30);
            ProcResult result=builder.run();
            long sizeInBytes = FileUtils.sizeOf(sqlFile);
            log.info("表{} dump完毕！size:{}mb",tableMeta.getTableName(),sizeInBytes/1024/1024);

            if(result.getExitValue()!=0)
            {
                throw new IllegalAccessException("mysql dump fail! exitValue:"+result.getExitValue()+" output:"+result.getOutputString());
            }


            if(!sqlFile.exists())
            {
                throw new ShouldNeverHappenException("mysqldump error,sql file is not exists!");
            }

            return sqlFile;
        }finally {
            DbUtil.close(dbConn);
        }
    }
}

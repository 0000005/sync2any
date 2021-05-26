package com.jte.sync2any.extract.impl;

import cn.hutool.db.DbUtil;
import cn.hutool.db.handler.StringHandler;
import cn.hutool.db.sql.SqlExecutor;
import com.jte.sync2any.exception.ShouldNeverHappenException;
import com.jte.sync2any.extract.DbMetaExtract;
import com.jte.sync2any.model.config.TargetDatasources;
import com.jte.sync2any.model.mysql.TableMeta;
import com.jte.sync2any.util.DbUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author JerryYin
 */
@Service
@Slf4j
public class CkMetaExtractImpl implements DbMetaExtract {

    public final String FIND_TABLE_ENGINE_SQL ="select engine from `system`.tables t  where t.database ='%s' and t.name ='%s'";

    @Resource
    @Qualifier("allTargetDatasource")
    Map<String,JdbcTemplate> allTargetDatasource;

    @Resource
    TargetDatasources targetDatasources;


    @Override
    public TableMeta getTableMate(String dbId,String tableName) {
        return null;
    }

    @Override
    public Long getDataCount(String dbId, String tableName) {
        return 0L;
    }

    @Override
    public List<String> getAllTableName(String dbId) {
        return null;
    }

    @Override
    public String getTableEngineName(String dbId, String tableName) {
        String dbName = DbUtils.getConnByDbId(targetDatasources.getDatasources(),dbId).getDbName();
        DataSource ds = (DataSource) allTargetDatasource.get(dbId);
        Connection conn = null;
        try {
            conn = ds.getConnection();
            log.info("sql:{}",String.format(FIND_TABLE_ENGINE_SQL, dbName, tableName));
            return SqlExecutor.query(conn, String.format(FIND_TABLE_ENGINE_SQL, dbName, tableName), new StringHandler());
        } catch (SQLException e) {
            log.error("execute sql error:",e);
            throw new ShouldNeverHappenException("execute sql error");
        } finally {
            DbUtil.close(conn);
        }
    }


}

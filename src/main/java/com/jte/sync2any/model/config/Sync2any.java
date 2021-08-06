package com.jte.sync2any.model.config;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
@Component
@ConfigurationProperties("sync2any")
@Data
public class Sync2any {
    /**
     * "mysqldump" command path
     */
    private String mysqldump;
    /**
     * "mysqldump" sql data save location
     */
    private String mysqldumpDataLocation;
    /**
     *  which mysql database need to sync.
     */
    private List<SyncConfig> syncConfigList;
    /**
     *  alert config
     */
    private Alert alert;

    /**
     * 通过topicGroup来找sync-config-list中的配置
     * @param topicGroup
     */
    public SyncConfig findSyncConfigByTopicGroup(String topicGroup)
    {
        if(StringUtils.isBlank(topicGroup))
        {
            return null;
        }
        SyncConfig syncConfig=this.getSyncConfigList().stream()
                .filter(e->topicGroup.equals(e.getMq().getTopicGroup()))
                .findFirst()
                .orElse(null);
        return syncConfig;
    }
}

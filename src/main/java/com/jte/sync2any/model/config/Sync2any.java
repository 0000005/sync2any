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
     *  which mysql database need to sync.
     */
    private List<SyncConfig> syncConfigList;
    /**
     *  alert config
     */
    private Alert alert;

    /**
     * 通过topicName来找sync-config-list中的配置
     * @param topicName
     */
    public SyncConfig findSyncConfigByTopicName(String topicName)
    {
        if(StringUtils.isBlank(topicName))
        {
            return null;
        }
        SyncConfig syncConfig=this.getSyncConfigList().stream()
                .filter(e->topicName.equals(e.getMq().getTopicName()))
                .findFirst()
                .orElse(null);
        return syncConfig;
    }
}

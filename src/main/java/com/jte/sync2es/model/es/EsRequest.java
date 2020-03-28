package com.jte.sync2es.model.es;

import com.jte.sync2es.model.mysql.TableMeta;
import lombok.Data;

import java.util.Map;

@Data
public class EsRequest {
    /**
     * 操作类型
     * Also see: com.jte.sync2es.extract.impl.KafkaMsgListener
     */
    private String operationType;

    /**
     * Es index
     */
    private String index;

    /**
     * documentId
     */
    private String docId;

    /**
     * row data
     */
    private Map<String, Object> parameters;

    private TableMeta tableMeta;
}

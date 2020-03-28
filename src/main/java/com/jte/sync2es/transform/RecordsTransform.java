package com.jte.sync2es.transform;

import com.jte.sync2es.model.es.EsRequest;
import com.jte.sync2es.model.mq.TcMqMessage;
import com.jte.sync2es.model.mysql.TableRecords;

/**
 * 用于解析mq收到的消息
 *
 */
public interface RecordsTransform {
    EsRequest transform(TableRecords records);
}

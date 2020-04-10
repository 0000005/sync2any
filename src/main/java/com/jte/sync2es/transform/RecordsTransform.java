package com.jte.sync2es.transform;

import com.jte.sync2es.model.es.EsRequest;
import com.jte.sync2es.model.mysql.TableRecords;

/**
 * 用于解析mq收到的消息
 *
 */
public interface RecordsTransform {
    /**
     * 考虑到MQ中的数据有“毛刺”，且数据库结构的变更在MQ消息中并不能明显的体现出值对应的字段。
     * 我们规定：
     * 1、当往数据库加字段时，必须将字段加到最末尾，切记不可将新字段插入旧的字段前。
     * 2、不支持删除字段。 //TODO 可以支持，需要新增一个fallback的变量保存之前的表结构。每10分钟只能删一个字段
     * 基于以上规定，我们才能够在数据库更改表结构的同时，平滑的同步数据。
     * 平滑同步数据的关键在于原数据源的字段与目标数据源的字段匹配。
     * 当原数据源新增字段时：
     * 由于“毛刺”现象的原因，MQ中会存在同时存在（无序）新表结构的数据，和旧表结构的数据。
     * 一旦发现MQ数据中字段数大于本地缓存表结构的字段数，则马上同步一次数据库结构（可能拉不到最新的表结构，会尝试3次）
     * 接下来按照最新的表结构来处理MQ中的数据。按照字段顺序匹配，如若匹配不上，则抛弃没匹配上的字段。保证至少匹配上的字段是正确的。
     *
     * 问题：
     * 1、假设字段没加到最后怎么处理？
     * 2、假设误删了字段怎么处理？
     *
     * @param records
     * @return
     */
    EsRequest transform(TableRecords records);
}

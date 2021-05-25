package com.jte.sync2any.config;

import com.jte.sync2any.conf.RuleConfigParser;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author JerryYin
 * @since 2021-05-24 17:14
 */
public class RuleConfigParserTest {
    @Test
    public void getTableNameFromDdlTest(){
        String tableName = new RuleConfigParser().getTableNameFromDdl("CREATE TABLE `test111112` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `group_code` varchar(64) NOT NULL,\n" +
                "  `hotel_code` varchar(32) NOT NULL,\n" +
                "  `order_code` varchar(32) DEFAULT NULL COMMENT '订单号',\n" +
                "  `operator` varchar(32) NOT NULL COMMENT '操作人',\n" +
                "  `guest_name` varchar(32) DEFAULT NULL COMMENT '客户姓名',\n" +
                "  `room_number` varchar(64) DEFAULT NULL COMMENT '房间号',\n" +
                "  `room_code` varchar(64) DEFAULT NULL COMMENT '房间代码',\n" +
                "  `log_content` json DEFAULT NULL COMMENT '日志（存个性化内容，可用于查询）',\n" +
                "  `biz_type` varchar(64) DEFAULT NULL COMMENT '业务类型（存文字）',\n" +
                "  `biz_detail` tinytext COMMENT '业务详情',\n" +
                "  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
                "  `update_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',\n" +
                "  PRIMARY KEY (`id`,`group_code`) USING BTREE,\n" +
                "  KEY `inx_gh` (`group_code`,`hotel_code`) USING BTREE\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=1553109 DEFAULT CHARSET=utf8mb4 COMMENT='营业日志表'");
        Assert.assertEquals("test111112",tableName);
    }
}

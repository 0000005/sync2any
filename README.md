sync2es用于将腾讯云TDSQL中的数据实时同步到Elasticsearch（7.x）。**注意，普通mysql服务并不适用。**

首先使用`mysqldump`同步原始数据，再读取CKAFKA中的队列消息实时同步到Elasticsearch中。

实时同步数据流为：TDSQL -> CKAFKA -> sync2es -> Elasticsearch

## 使用先决条件
- 购买使用了腾讯云提供的TDSQL
- 购买了腾讯云提供的CKAFKA

## 使用限制
因为腾讯云本身对外提供的接口能力不足，导致我们做功能的时候不得不做一些取舍和限制才能保证数据的准确。限制如下：

- 对于正在同步的表，暂不支持删除已有字段。
- 对于正在同步的表，新增字段必须加到表的末尾，千万不能插到表已有字段中间。
- 对于正在同步的表，新增字段暂不支持同步到es端。
- 当TDSQL扩容新增物理节点时，数据同步将会异常。
- 表中必须要有主键

对于1、2点的限制原因是腾讯云投递过来的消息并不包含字段名称信息，所以一旦修改表结构，那么投递过来的值将无法和对应的字段匹配上。所以一旦违规操作，将会导致es端数据混乱。

对于第4点限制，腾讯云反馈已有开发计划，但是不知什么时候会上线。

建议大家都去提提工单，让腾讯云加强这方面的功能。

## 配置与安装
### 腾讯云端的配置
1. 进入CKAFKA中，新建一个`topic`。如果对于数据同步的准确性要求高，建议设置分区数（partition）为1。
2. 进入MariaDB的菜单，找到数据同步模块（别问我为什么TDSQL的数据同步放到了MariaDB里面，），新建同步任务，将数据投递到刚刚创建的`topic`中。
### 本地同步服务的启动 
可以直接使用jar包，也可以使用docker的形式来启动。
#### 本地启动
1. 安装好`mysqldump`，并且配置到配置文件
2. 
#### docker启动

### 配置文件详解
目前项目启动时暂未做对配置文件的校验，因此错误的配置可能会导致项目启动报错。配置文件采用yml格式，不熟悉的同学可以先学习一下。
```yaml
#【必填】同步目标elasticsearch的基本配置
elasticsearch:
    uris: 192.168.10.208:9200
    username: elastic
    password: changeme

#【必填】腾讯云CKAFKA配置
kafka:
  adress: 123.207.61.134:32768

#【必填】tdsql配置，可以配置多个数据库
mysql:
  datasources:
    -
      db-name: jte_pms_member
      url: jdbc:mysql://127.0.0.1:3306/jte_pms_member?useUnicode=true&useSSL=false&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false&useOldAliasMetadataBehavior=true&allowMultiQueries=true&serverTimezone=Hongkong
      username: test
      password: test
      driver-class-name: com.mysql.cj.jdbc.Driver

#【必填】配置同步到elasticsearch的基本规则
sync2es:
  #【必填】mysqldump工具的地址
  mysqldump: D:\program\mysql-5.7.25-winx64\bin\mysqldump.exe
  # 规则比较灵活，可以配置多个
  sync-config-list:
    -
      # 要同步的TDSQL数据库名称
      db-name: jte_pms_member
      # 要同步的表名，支持正则表达式，多个表名用逗号分隔
      sync-tables: "t_pms_member,t_pms_member_order_[0-9]{10}"
      mq:
        # 监听的CKAFKA的topic名称
        topic-name: test-t_pms_member
        #【选填】消费者使用的topicGroup，如果不填写，则随机生成。每次重启本应用都会从kafka的"earliest"处开始读取。
        topic-group: local-test-consumer-group
      # 【选填】此处可以配置TDSQL到elasticsearch的映射规则
      rules:
        -
          # 匹配此rule的表名，支持正则表达式
          table: t_pms_member_order_[0-9]{10}
          # 自定义es的index名称
          index: t_pms_member_order
          # 自定义同步到es的字段名称和字段类型(es的类型)，字段类型请参考类：com.jte.sync2es.model.es.EsDateType
          map: '{"group_code":"groupCode","hotel_code":"hotelCode,integer","user_code":",integer"}'
          # 字段过滤，多个字段用逗号分隔。如果有值，则只保留这里填写的字段。
          fieldFilter: "user_id,user_name"
```
## 其他碎碎念
### 一些约定
- es的index默认命名规则为“数据库名-表名”,es那边的字段名和tdsql保持一致。默认所有同步过去的名称都会转化为小写。
- 一旦同步过程中发生任何错误，都会停止继续同步，防止数据错乱。其他正常的任务可继续执行，不影响。建议排除故障后，重启sync2es继续同步。


### 最佳实践
- CKAFKA创建topic时，一定要将`max.message.bytes`设置为最大值8MB。否则流量高峰时会有TDSQL投递消息到CKAFKA失败的情况发生。
- 建议创建TOPIC时设置partition为1，否则可能导致数据错乱。

### 性能
不同的机器性能不一样，要算每个TOPIC的QPS,可以通过控制面板的tpq参数计算。也就是1000（ms）/tpq=QPS。

### 参与项目开发


### 感谢


### 捐助


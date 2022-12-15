# flink-sql-security

FlinkSQL的行级权限解决方案及源码，支持面向个人级别的行级数据访问控制，即特定用户只能访问授权过的行，隐藏未授权的行数据。此方案是实时领域Flink的解决方案，类似离线数仓Hive中Ranger Row-level Filter方案。

<br/>

| 序号 | 作者 | 版本 | 时间 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | HamaWhite | 1.0.0 | 2022-12-15 | 1. 增加文档和源码 |


</br>
源码地址: https://github.com/HamaWhiteGG/flink-sql-security

作者邮箱: song.bs@dtwave-inc.com


## 一、基础知识
### 1.1 行级权限
行级权限即横向数据安全保护，可以解决不同人员只允许访问不同数据行的问题。例如针对订单表，用户A只能查看到北京区域的数据，用户B只能杭州区域的数据。
![Row level permissions.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/main/data/images/Row%20level%20permissions.png)

### 1.2 业务流程
#### 1.2.1 设置行级权限
管理员配置用户、表、行级权限条件，例如下面的配置。
| 序号 |  用户名 | 表名 | 行级权限条件 | 
| --- | --- | --- | --- | 
| 1 | 用户A | orders | region = 'beijing' | 
| 2 | 用户B | orders | region = 'hangzhou' | 


#### 1.2.2 用户查询数据
用户在系统上查询orders表数据时，系统在底层查询数据时会根据用户的行级权限条件来过滤数据，即让行级权限自动生效。

当用户A和用户B均执行下述SQL时，

```sql
SELECT * FROM orders;
```

**用户A查看到的结果数据**:
| order_id |  order_date | customer_name | price |  product_id |  order_status |  region | 
| --- | --- | --- | --- |  --- |  --- |  --- | 
| 10001 | 2020-07-30 10:08:22 | Jack | 50.50 | 102 | false | beijing |
| 10002 | 2020-07-30 10:11:09 | Sally | 15.00 | 105 | false | beijing | 
> 注: 系统底层执行的最终SQL是: SELECT * FROM orders WHERE region = 'beijing' 。 

<br/>

**用户B查看到的结果数据**:
| order_id |  order_date | customer_name | price |  product_id |  order_status |  region |  
| --- | --- | --- | --- |  --- |  --- |  --- | 
| 10003 | 2020-07-30 12:00:30 | Edward | 25.25 | 106 | false | hangzhou | 
| 10004 | 2022-12-15 12:11:09 | John | 78.00 | 103 | false | hangzhou | 
> 注: 系统底层执行的最终SQL是: SELECT * FROM orders WHERE region = 'hangzhou' 。 
> 
### 1.3 组件版本
| 组件名称 | 版本 | 备注 |
| --- | --- | --- |
| Flink | 1.16.0 |  
| Mysql | 8.0.29 |  |
| Flink-connector-mysql-cdc | 2.3.0 |  |
| JDK | 1.8 | |
| Scala | 2.12 |  |

## 二、Hive行级权限解决方案
在离线数仓工具Hive领域，由于发展多年，已经有Ranger来支持表数据的行级权限控制，详见参考文献[[2]](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.0/authorization-ranger/content/row_level_filtering_in_hive_with_ranger_policies.html)。下图是在Ranger里配置Hive表行级过滤条件的页面，供参考。
![Hive-Ranger row level filter.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/main/data/images/Hive-Ranger%20row%20level%20filter.png)

<br/>

由于Flink实时数仓领域发展相对较短，Ranger还不支持FlinkSQL，因此开始自研实时数仓的行级权限解决工具。

## 三、FlinkSQL行级权限解决方案

## 四、用例测试

## 五、源码修改步骤

## 六、参考文献
1. [数据管理DMS-敏感数据管理-行级管控](https://help.aliyun.com/document_detail/161149.html)
2. [Apache Ranger Row-level Filter](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.0/authorization-ranger/content/row_level_filtering_in_hive_with_ranger_policies.html)
3. [OpenLooKeng的行级权限控制](https://www.modb.pro/db/212124)


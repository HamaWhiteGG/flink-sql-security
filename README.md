# flink-sql-security

FlinkSQL的行级权限解决方案及源码，支持面向个人级别的行级数据访问控制，类似于Ranger Row-level Filter(Hive)。

<br/>

| 序号 | 作者 | 版本 | 时间 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | HamaWhite | 1.0.0 | 2022-12-15 | 1. 增加文档和源码 |


</br>
源码地址: https://github.com/HamaWhiteGG/flink-sql-security
作者邮箱: song.bs@dtwave-inc.com



## 一、基础知识
### 行级权限
行级权限即横向数据安全保护，可以解决不同人员只允许访问不同数据行的问题。例如针对订单表，用户1只能查看到北京区域的数据，用户2只能杭州区域的数据。
![Row level permissions.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/main/data/images/Row%20level%20permissions.png)




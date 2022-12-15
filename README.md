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

### 业务流程

### 组件版本

## 示例: 用户A查看北京的数据，用户B查看杭州的数据。

## 二、Hive行级权限
Ranger-Row-level Filtering


## 三、FlinkSQL行级权限
3.1 解决方案
输入SQL解析生成AST
过滤经过parseExpression生成SqlBasicCall

在SqlSelect中根据AST和SqlBasicCall重新组合生成新where条件。
得到新的SQL
3.2 核心源码
补充流程图

## 四、单元测试
 测试数据来自于基于 Flink CDC 构建 MySQL 和 Postgres 的 Streaming ETL，给orders增加region字段，以及多增加三行数据
### 4.1 新建Mysql表及初始化数据
### 4.2 新建Flink表
Mysql新建表语句及初始化SQL详见 xxx地址
### 4.3 测试用例

详细测试用例可查看源码中的单测，下面只描述五个测试点。

#### 4.3.1 简单SELECT
#### 4.3.2 SELECT带WHERE约束
#### 4.3.3 两表JOIN(testJoinWithBothPermissions)
#### 4.3.4 三表JOIN(testThreeJoin)
#### 4.3.5 INSERT来自带子查询的SELECT

## 五、源码主要步骤
1.  修改org.apache.flink.table.delegation.Parser增加两个方法
        /**
     * Parses a SQL expression into a {@link SqlNode}. The {@link SqlNode} is not yet validated.
     *
     * @param sqlExpression a SQL expression string to parse
     * @return a parsed SQL node
     * @throws SqlParserException if an exception is thrown when parsing the statement
     */
    SqlNode parseExpression(String sqlExpression);


    /**
     * Entry point for parsing SQL queries and return the abstract syntax tree
     *
     * @param statement the SQL statement to evaluate
     * @return abstract syntax tree
     * @throws org.apache.flink.table.api.SqlParserException when failed to parse the statement
     */
    SqlNode parseSql(String statement);


org.apache.flink.table.planner.delegation.ParserImpl 的实现如下:


2. 根据parseExpression生成
 /**
     * Query the configured permission point according to the user name and table name, and return
     * it to SqlBasicCall
     */
    public SqlBasicCall queryPermissions(String username, String tableName) {
        String permissions = rowLevelPermissions.get(username, tableName);
        LOG.info("username: {}, tableName: {}, permissions: {}", username, tableName, permissions);
        if (permissions != null) {
            return (SqlBasicCall) tableEnv.getParser().parseExpression(permissions);
        }
        return null;
    }


3. 修改org.apache.calcite.sql.SqlSelect
在构造方法中，注释掉原有的  this.where = where 行，通过自定义的addCondition(SqlNode from, SqlNode where, boolean fromJoin) 方法生成新的where约束。

4. 调用tableEnv.getParser().parseSql得到新的SQL


    /**
     * Add row-level filter conditions and return new SQL
     */
    public String addRowLevelFilter(String username, String singleSql) {
        System.setProperty(SECURITY_USERNAME, username);

        // in the modified SqlSelect, filter conditions will be added to the where clause
        SqlNode parsedTree = tableEnv.getParser().parseSql(singleSql);
        return parsedTree.toString();
    }
## 六、参考文献



7. [数据管理DMS-敏感数据管理-行级管控](https://help.aliyun.com/document_detail/161149.html)
7. [Apache Ranger Row-level Filter](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.0/authorization-ranger/content/row_level_filtering_in_hive_with_ranger_policies.html)




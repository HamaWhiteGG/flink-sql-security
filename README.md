# flink-sql-security
FlinkSQL数据安全-行级权限，类似Ranger Row-level Filtering

书写中...


## 一、行级权限介绍

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

## 四、单元测试
### 4.1 新建Mysql表
### 4.2 新建Flink表
Mysql新建表语句及初始化SQL详见 xxx地址
### 4.3 测试用例
详细测试用例
#### 4.3.1 简单SELECT
#### 4.3.2 SELECT带WHERE约束
#### 4.3.3 两表JOIN(testJoinWithBothPermissions)
#### 4.3.4 三表JOIN(testThreeJoin)
#### 4.3.5 INSERT来自带子查询的SELECT

五、源码修改点
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



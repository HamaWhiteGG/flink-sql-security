# Flink SQL 数据脱敏

FlinkSQL的数据脱敏解决方案及源码，支持面向用户级别的数据脱敏访问控制，即特定用户只能访问到脱敏后的数据。
此方案是实时领域Flink的解决方案，类似于离线数仓Hive中Ranger Column Masking方案。

<br/>
源码地址: https://github.com/HamaWhiteGG/flink-sql-security 

> 注: 如果用IntelliJ IDEA打开源码，请提前安装 **Manifold** 插件。


## 一、基础知识
### 1.1 数据脱敏
数据脱敏(Data Masking)是一种数据安全技术，用于保护敏感数据，以防止未经授权的访问。该技术通过将敏感数据替换为虚假数据或不可识别的数据来实现。
例如可以使用数据脱敏技术将信用卡号码、社会安全号码等敏感信息替换为随机生成的数字或字母，以保护这些信息的隐私和安全。

### 1.2 业务流程
下面用订单表`orders`的两行数据来举例，示例数据如下:
![Data mask example data.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/dev/docs/images/Data%20mask%20example%20data.png)

#### 1.2.1 设置脱敏策略
管理员配置用户、表、字段、脱敏条件，例如下面的配置。

| 序号 |  用户名 | 表名 | 字段名 | 脱敏条件 | 备注 | 
| --- | --- | --- | --- | --- |  --- | 
| 1 | 用户A | orders | customer_name | MASK | 掩盖全部字符 | 
| 2 | 用户B | orders | customer_name | MASK_SHOW_FIRST_4 | 仅显示前4个字符，其他用x代替 |

#### 1.2.2 用户访问数据
当用户在Flink上查询`orders`表的数据时，会在底层结合该用户的脱敏条件重新生成SQL，即让数据脱敏生效。
当用户A和用户B在执行下面相同的SQL时，会看到不同的结果数据。
```sql
SELECT * FROM orders
```

**用户A查看到的结果数据如下**，`customer_name`字段的数据被全部掩盖掉。
![Data mask-masked with customer_name after mask.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/dev/docs/images/Data%20mask-masked%20with%20customer_name%20after%20mask.png)

<br/>

**用户B查看到的结果数据如下**，`customer_name`字段的数据只会显示前4位，剩下的用x代替。
![Data mask-masked with customer_name after mask_show_first_4.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/dev/docs/images/Data%20mask-masked%20with%20customer_name%20after%20mask_show_first_4.png)


## 二、Hive数据脱敏解决方案
在离线数仓工具Hive领域，由于发展多年已有Ranger来支持字段数据的数据脱敏控制，详见参考文献[[1]](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.0/authorization-ranger/content/dynamic_resource_based_column_masking_in_hive_with_ranger_policies.html)。
下图是在Ranger里配置Hive表数据脱敏条件的页面，供参考。
![Hive-Ranger data mask.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/dev/docs/images/Hive-Ranger%20data%20mask.png)

<br/>
但由于Flink实时数仓领域发展相对较短，Ranger还不支持FlinkSQL，以及依赖Ranger的话会导致系统部署和运维过重，因此开始**自研实时数仓的数据脱敏解决工具**。
当然本文中的核心思想也适用于Ranger中，可以基于此较快开发出ranger-flink插件。

## 三、FlinkSQL数据脱敏解决方案
### 3.1 解决方案
#### 3.1.1 FlinkSQL执行流程
可以参考作者文章[[FlinkSQL字段血缘解决方案及源码]](https://github.com/HamaWhiteGG/flink-sql-lineage/blob/main/README_CN.md)，本文根据Flink1.16修正和简化后的执行流程如下图所示。
![FlinkSQL simple-execution flowchart.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/dev/docs/images/FlinkSQL%20simple-execution%20flowchart.png)

在`CalciteParser.parse()`处理后会得到一个SqlNode类型的抽象语法树，本文会针对此抽象语法树来组装脱敏条件后来生成新的AST，以实现数据脱敏控制。

#### 3.1.2 Calcite对象继承关系
下面章节要用到Calcite中的SqlNode、SqlCall、SqlIdentifier、SqlJoin、SqlBasicCall和SqlSelect等类，此处进行简单介绍以及展示它们间继承关系，以便读者阅读本文源码。

| 序号 | 类 | 介绍 |
| --- | --- | --- |
| 1 | SqlNode | A SqlNode is a SQL parse tree. |
| 2 | SqlCall | A SqlCall is a call to an SqlOperator operator. |
| 3 | SqlIdentifier | A SqlIdentifier is an identifier, possibly compound. |
| 4 | SqlJoin | Parse tree node representing a JOIN clause. |
| 5 | SqlBasicCall | Implementation of SqlCall that keeps its operands in an array. |
| 6 | SqlSelect | A SqlSelect is a node of a parse tree which represents a select statement, the parent class is SqlCall |

![Calcite SqlNode diagrams.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/dev/docs/images/Calcite%20SqlNode%20diagrams.png)

#### 3.1.3 解决思路

针对输入的Flink SQL，在`CalciteParser.parse()`进行语法解析后生成抽象语法树(`Abstract Syntax Tree`，简称AST)后，采用自定义
`Calcite SqlBasicVisitor`的方法遍历AST中的所有`SqlSelect`，获取到里面的每个输入表。如果输入表中字段有配置脱敏条件，则针对输入表生成子查询语句，
并把脱敏字段改写成`CAST(脱敏函数(字段名) AS 字段类型) AS 字段名`,再通过`CalciteParser.parseExpression()`把子查询转换成SqlSelect，
并用此SqlSelect替换原AST中的输入表来生成新的AST，最后得到新的SQL来继续执行。
![FlinkSQL data mask solution.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/dev/docs/images/FlinkSQL%20data%20mask%20solution.png)

### 3.2 详细方案
#### 3.2.1 如何解析出输入表
通过对Flink SQL 语法的分析和研究，最终出现输入表的只包含以下两种情况:
1. SELECT 语句的FROM子句，如果是子查询，则递归继续遍历。
2. SELECT ... JOIN 语句的Left和Right子句，如果是多表JOIN，则递归查询遍历。

因此，下面的主要步骤会根据FROM子句的类型来寻找输入表。

#### 3.2.2 主要步骤
主要通过Calcite提供的访问者模式自定义DataMaskVisitor来实现，遍历AST中所有的SqlSelect对象用子查询替换里面的输入表。
下面详细描述替换输入表的步骤，整体流程如下图所示。

![Data mask-rewrite the main process.png](https://github.com/HamaWhiteGG/flink-sql-security/blob/dev/docs/images/Row-level%20Filter-Rewrite%20the%20main%20process.png)

1. 遍历AST中SELECT语句。
2. 判断是否自定义的SELECT语句(由下面步骤10生成)，是则跳转到步骤11，否则继续步骤3。
3. 判断SELECT语句中的FROM类型，按照不同类型对应执行下面的步骤4、5、6和11。
4. 如果FROM是SqlJoin类型，则分别遍历其左Left和Right右节点，即执行当前步骤4和步骤7。由于可能是三张表及以上的Join，因此进行递归处理，即针对其左Left节点跳回到步骤3。
5. 如果FROM是SqlIdentifier类型，则表示是表。但是输入SQL中没有定义表的别名，则用表名作为别名。跳转到步骤8。
6. 如果FROM是SqlBasicCall类型，判断是否来自子查询，是则跳转到步骤11继续遍历AST，后续步骤1会对子查询中的SELECT语句进行处理。否则跳转到步骤8。
7. 递归处理Join的右节点，即跳回到步骤3。
8. 遍历表中的每个字段，如果某个字段有定义脱敏条件，则把改字段改写成格式`CAST(脱敏函数(字段名) AS 字段类型) AS 字段名`，否则用原字段名。
9. 针对步骤8处理后的字段，构建子查询语句，形如 `(SELECT 字段名1, 字段名2, CAST(脱敏函数(字段名3) AS 字段类型) AS 字段名3、字段名4 FROM 表名) AS 表别名`。
10. 对步骤9的子查询调用`CalciteParser.parseExpression()`进行解析，生成自定义SELECT语句，并替换掉原FROM。
11. 继续遍历AST，找到里面的SELECT语句进行处理，跳回到步骤1。

#### 3.2.3 Hive及Ranger兼容性
在Ranger中，默认的脱敏策略的如下所示。通过调研发现Ranger的大部分脱敏策略是通过调用Hive自带或自定义的系统函数实现的。

| 序号 | 策略名 | 策略说明 | Hive系统函数 |
| --- | --- | --- | --- |
| 1 | Redact | 用x屏蔽字母字符，用n屏蔽数字字符 | mask |
| 2 | Partial mask: show last 4 | 仅显示最后四个字符,其他用x代替 | mask_show_last_n |
| 3 | Partial mask: show first 4 | 仅显示前四个字符,其他用x代替 | mask_show_first_n |
| 4 | Hash | 用值的哈希值替换原值 | mask_hash |
| 5 | Nullify | 用NULL值替换原值 | Ranger自身实现 |
| 6 | Unmasked | 原样显示 | Ranger自身实现 |
| 7 | Date: show only year | 仅显示日期字符串的年份 | mask |
| 8 | Custom | Hive UDF来自定义策略 | |

由于Flink支持Hive Catalog，在Fink能调用Hive系统函数。 因此，本方案也支持在Flink SQL配置Ranger的脱敏策略。

## 四、用例测试
用例测试数据来自于CDC Connectors for Apache Flink
[[4]](https://ververica.github.io/flink-cdc-connectors/master/content/%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B/mysql-postgres-tutorial-zh.html)官网，
本文给`orders`表增加一个region字段，同时增加`'connector'='print'`类型的print_sink表，其字段和`orders`表的一样。

下载本文源码后，可通过Maven运行单元测试。
```shell
$ cd flink-sql-security
$ mvn test
```

详细测试用例可查看源码中的单测`RewriteDataMaskTest`和`ExecuteDataMaskTest`，下面只描述两个案例。

### 4.1 测试SELECT
#### 4.1.1 输入SQL
用户A执行下述SQL: 
```sql
SELECT order_id, customer_name, product_id, region FROM orders
```
#### 4.1.2 根据脱敏条件重新生成SQL
1. 输入SQL是一个简单SELECT语句，其FROM类型是`SqlIdentifier`，由于没有定义别名，用表名`orders`作为别名。
2. 由于用户A针对字段`customer_name`定义脱敏条件MASK(对应函数是脱敏函数是`mask`)，该字段在流程图中的步骤8中被改写为`CAST(mask(customer_name) AS STRING) AS customer_name`，其余字段未定义脱敏条件则保持不变。
3. 然后在步骤9的操作中，表名`orders`被改写成如下子查询，子查询两侧用括号`()`进行包裹，并且用 `AS 别名`来增加表别名。

```sql
(SELECT
     order_id,
     order_date,
     CAST(mask(customer_name) AS STRING) AS customer_name,
     product_id,
     price,
     order_status,
     region
FROM 
    orders
) AS orders
```
#### 4.1.3 输出SQL和运行结果
最终执行的改写后SQL如下所示，这样用户A查询到的顾客姓名`customer_name`字段都是掩盖后的数据。
```sql
SELECT
    order_id,
    customer_name,
    product_id,
    region
FROM (
    SELECT 
         order_id,
         order_date,
         CAST(mask(customer_name) AS STRING) AS customer_name,
         product_id,
         price,
         order_status,
         region
    FROM 
         orders
     ) AS orders
```

### 4.2 测试INSERT-SELECT
#### 4.2.1 输入SQL
用户A执行下述SQL:
```sql
INSERT INTO print_sink SELECT * FROM orders
```
#### 4.2.2 根据脱敏条件重新生成SQL
通过自定义Calcite DataMaskVisitor访问生成的AST，能找到对应的SELECT语句是`SELECT order_id, customer_name, product_id, region FROM orders`。

针对此SELECT语句的改写逻辑同上，不再阐述。

#### 4.2.3 输出SQL和运行结果
最终执行的改写后SQL如下所示，注意插入到`print_sink`表的`customer_name`字段是掩盖后的数据。
```sql
INSERT INTO print_sink (
    SELECT 
        * 
    FROM (
        SELECT 
            order_id, 
            order_date, 
            CAST(mask(customer_name) AS STRING) AS customer_name, 
            product_id, 
            price, 
            order_status, 
            region 
        FROM 
            orders
    ) AS orders
)
```

## 五、参考文献
1. [Apache Ranger Column Masking in Hive](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.0/authorization-ranger/content/dynamic_resource_based_column_masking_in_hive_with_ranger_policies.html)
2. [FlinkSQL字段血缘解决方案及源码](https://github.com/HamaWhiteGG/flink-sql-lineage/blob/main/README_CN.md)
3. [从SQL语句中解析出源表和结果表](https://blog.jrwang.me/2018/parse-table-in-sql)
4. [基于Flink CDC构建MySQL和Postgres的Streaming ETL](https://ververica.github.io/flink-cdc-connectors/master/content/%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B/mysql-postgres-tutorial-zh.html)
5. [HiveQL—数据脱敏函数](https://blog.csdn.net/CPP_MAYIBO/article/details/104065839)

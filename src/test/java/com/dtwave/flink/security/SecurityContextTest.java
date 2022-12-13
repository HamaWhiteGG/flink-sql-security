package com.dtwave.flink.security;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * @description: SecurityContextTest
 * @author: baisong
 * @version: 1.0.0
 * @date: 2022/12/10 12:24 PM
 */
public class SecurityContextTest {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityContextTest.class);

    private static final SecurityContext context = SecurityContext.getInstance();

    private static final String FIRST_USER = "hamawhite";
    private static final String SECOND_USER = "song.bs";
    private static final String ORDERS_TABLE = "orders";

    @BeforeClass
    public static void init() {
        // set row level permissions
        Table<String, String, String> rowLevelPermissions = HashBasedTable.create();
        rowLevelPermissions.put(FIRST_USER, ORDERS_TABLE, "region = 'beijing'");
        rowLevelPermissions.put(SECOND_USER, ORDERS_TABLE, "region = 'hangzhou'");
        context.setRowLevelPermissions(rowLevelPermissions);

        // create mysql cdc table orders
        createTableOfOrders();
    }

    /**
     * Only select, no where clause
     */
    @Test
    public void testSelect() {
        String input = "SELECT * FROM orders";
        String expected = "SELECT * FROM orders WHERE region = 'beijing'";

        testRowLevelFilter(FIRST_USER, input, expected);
    }


    /**
     * Different users configure different permission points
     */
    @Test
    public void testSelectDiffUser() {
        String input = "SELECT * FROM orders";
        String firstExpected = "SELECT * FROM orders WHERE region = 'beijing'";
        String secondExpected = "SELECT * FROM orders WHERE region = 'hangzhou'";

        testRowLevelFilter(FIRST_USER, input, firstExpected);
        testRowLevelFilter(SECOND_USER, input, secondExpected);
    }

    /**
     * Where there is a condition
     */
    @Test
    public void testSelectWhere() {
        String input = "SELECT * FROM orders WHERE price > 120";
        String expected = "SELECT * FROM orders WHERE price > 120 AND region = 'beijing'";

        testRowLevelFilter(FIRST_USER, input, expected);
    }

    /**
     * Where there is complex condition,
     * add a pair of parentheses to the existing multiple where conditions
     */
    @Test
    public void testSelectComplexWhere() {
        String input = "SELECT * FROM orders WHERE price > 120 OR customer_name = 'John'";
        String expected = "SELECT * FROM orders WHERE (price > 120 OR customer_name = 'John') AND region = 'beijing'";

        testRowLevelFilter(FIRST_USER, input, expected);
    }

    /**
     * With group by clause
     */
    @Test
    public void testSelectWhereGroupBy() {
        String input = "SELECT customer_name, count(*) AS cnt FROM orders WHERE price > 120 GROUP BY customer_name";
        String expected = "SELECT customer_name, COUNT(*) AS cnt FROM orders WHERE price > 120 AND region = 'beijing' GROUP BY customer_name";

        testRowLevelFilter(FIRST_USER, input, expected);
    }


    private void testRowLevelFilter(String username, String inputSql, String expectedSql) {
        LOG.info("----------------------------------------");
        String resultSql = context.addRowLevelFilter(username, inputSql);
        // replace \n with spaces and remove single apostrophes
        resultSql = resultSql.replace("\n", " ").replace("`", "");

        LOG.info("Input SQL: {}", inputSql);
        LOG.info("Result SQL: {}", resultSql);
        assertEquals(expectedSql, resultSql);

    }


    /**
     * Create mysql cdc table orders
     */
    private static void createTableOfOrders() {
        context.execute("DROP TABLE IF EXISTS " + ORDERS_TABLE);

        context.execute("CREATE TABLE IF NOT EXISTS " + ORDERS_TABLE + " (" +
                "       order_id            INT PRIMARY KEY NOT ENFORCED ," +
                "       order_date          TIMESTAMP(0)                 ," +
                "       customer_name       STRING                       ," +
                "       product_id          INT                          ," +
                "       price               DECIMAL(10, 5)               ," +
                "       region              STRING                        " +
                ") WITH ( " +
                "       'connector' = 'mysql-cdc'            ," +
                "       'hostname'  = '192.168.90.150'       ," +
                "       'port'      = '3306'                 ," +
                "       'username'  = 'root'                 ," +
                "       'password'  = 'root@123456'          ," +
                "       'server-time-zone' = 'Asia/Shanghai' ," +
                "       'database-name' = 'demo'             ," +
                "       'table-name'    = 'orders' " +
                ")"
        );
    }
}
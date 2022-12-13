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
    private static final String TABLE_NAME = "orders";

    @BeforeClass
    public static void init() {
        // set row level permissions
        Table<String, String, String> rowLevelPermissions = HashBasedTable.create();
        rowLevelPermissions.put(FIRST_USER, TABLE_NAME, "region='beijing'");
        rowLevelPermissions.put(SECOND_USER, TABLE_NAME, "region='hangzhou'");
        context.setRowLevelPermissions(rowLevelPermissions);

        // create mysql cdc table orders
        createTableOfOrders();
    }

    @Test
    public void testSelect() {
        String input = "SELECT * FROM orders";
        String expected = "SELECT * FROM orders WHERE region = 'beijing'";

        testRowLevelFilter(FIRST_USER, input, expected);
    }

    @Test
    public void testSelectWhere() {
        String input = "SELECT * FROM orders WHERE price > 120";
        String expected = "SELECT * FROM orders WHERE price > 120 AND region = 'beijing'";

        testRowLevelFilter(FIRST_USER, input, expected);
    }

    @Test
    public void testSelectComplexWhere() {
        String input = "SELECT * FROM orders WHERE price > 120 OR customer_name = 'John'";
        String expected = "SELECT * FROM orders WHERE (price > 120 OR customer_name = 'John') AND region = 'beijing'";

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
        context.execute("DROP TABLE IF EXISTS " + TABLE_NAME);

        context.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
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
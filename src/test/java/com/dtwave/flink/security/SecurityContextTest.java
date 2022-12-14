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
    private static final String PRODUCTS_TABLE = "products";
    private static final String SHIPMENTS_TABLE = "shipments";

    @BeforeClass
    public static void init() {
        // set row level permissions
        Table<String, String, String> rowLevelPermissions = HashBasedTable.create();
        rowLevelPermissions.put(FIRST_USER, ORDERS_TABLE, "region = 'beijing'");
        rowLevelPermissions.put(SECOND_USER, ORDERS_TABLE, "region = 'hangzhou'");
        context.setRowLevelPermissions(rowLevelPermissions);

        // create mysql cdc table orders
        createTableOfOrders();

        // create mysql cdc table products
        createTableOfProducts();

        // create mysql cdc table shipments
        createTableOfShipments();
    }

    /**
     * Only select, no where clause
     */
    @Test
    public void testSelect() {
        String inputSql = "SELECT * FROM orders";
        String expected = "SELECT * FROM orders WHERE region = 'beijing'";

        testRowLevelFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * Different users configure different permission points
     */
    @Test
    public void testSelectDiffUser() {
        String inputSql = "SELECT * FROM orders";
        String firstExpected = "SELECT * FROM orders WHERE region = 'beijing'";
        String secondExpected = "SELECT * FROM orders WHERE region = 'hangzhou'";

        testRowLevelFilter(FIRST_USER, inputSql, firstExpected);
        testRowLevelFilter(SECOND_USER, inputSql, secondExpected);
    }

    /**
     * Where there is a condition
     */
    @Test
    public void testSelectWhere() {
        String inputSql = "SELECT * FROM orders WHERE price > 45.0";
        String expected = "SELECT * FROM orders WHERE price > 45.0 AND region = 'beijing'";

        testRowLevelFilter(FIRST_USER, inputSql, expected);
    }

    /**
     * Where there is complex condition, add a pair of parentheses to the existing multiple where
     * conditions
     */
    @Test
    public void testSelectComplexWhere() {
        String inputSql = "SELECT * FROM orders WHERE price > 45.0 OR customer_name = 'John'";
        String expected = "SELECT * FROM orders WHERE (price > 45.0 OR customer_name = 'John') AND region = 'beijing'";

        testRowLevelFilter(FIRST_USER, inputSql, expected);
    }

    /**
     * With group by clause
     */
    @Test
    public void testSelectWhereGroupBy() {
        String inputSql = "SELECT customer_name, count(*) AS cnt FROM orders WHERE price > 45.0 GROUP BY customer_name";
        String expected = "SELECT customer_name, COUNT(*) AS cnt FROM orders WHERE price > 45.0 AND region = 'beijing' GROUP BY customer_name";

        testRowLevelFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * The two tables of products and orders are left joined
     */
    @Test
    public void testJoin() {
        String inputSql = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id";
        String expected = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE o.region = 'beijing'";

        testRowLevelFilter(FIRST_USER, inputSql, expected);
    }

    /**
     * The two tables of products and orders are left joined, but without alias
     */
    @Test
    public void testJoinWithoutAlias() {
        String inputSql = "SELECT o.*, p.name, p.description FROM orders LEFT JOIN products ON orders.product_id = products.id";
        String expected = "SELECT o.*, p.name, p.description FROM orders LEFT JOIN products ON orders.product_id = products.id WHERE orders.region = 'beijing'";

        testRowLevelFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * The two tables of products and orders are left joined, and there is a condition
     */
    @Test
    public void testJoinWhere() {
        String inputSql = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE o.price > 45.0 OR o.customer_name = 'John'";
        String expected = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE (o.price > 45.0 OR o.customer_name = 'John') AND o.region = 'beijing'";

        testRowLevelFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * The products and orders two tables are left joined, and the left table comes from a subquery
     */
    @Test
    public void testJoinSubQueryWhere() {
        String inputSql = "SELECT o.*, p.name, p.description FROM (SELECT * FROM orders WHERE order_status = false) AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE o.price > 45.0 OR o.customer_name = 'John'";
        String expected = "SELECT o.*, p.name, p.description FROM (SELECT * FROM orders WHERE order_status = FALSE AND region = 'beijing') AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE o.price > 45.0 OR o.customer_name = 'John'";
        testRowLevelFilter(FIRST_USER, inputSql, expected);
    }

    /**
     * The two tables of orders and products are joined, and both have row-level filter conditions
     */
    @Test
    public void testJoinWithBothPermissions() {
        // add permission
        context.getRowLevelPermissions().put(FIRST_USER, PRODUCTS_TABLE, "name = 'hammer'");
        String inputSql = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id";
        String expected = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE o.region = 'beijing' AND p.name = 'hammer'";
        testRowLevelFilter(FIRST_USER, inputSql, expected);

        // delete permission
        context.getRowLevelPermissions().remove(FIRST_USER, PRODUCTS_TABLE);
    }



    /**
     * The order table order, the product table products, and the logistics information table shipments
     * are associated with the three tables
     */
    @Test
    public void testThreeJoin() {
        // add permission
        context.getRowLevelPermissions().put(FIRST_USER, PRODUCTS_TABLE, "name = 'hammer'");
        context.getRowLevelPermissions().put(FIRST_USER, SHIPMENTS_TABLE, "is_arrived = false");

        String inputSql = "SELECT o.*, p.name, p.description, s.shipment_id, s.origin, s.destination, s.is_arrived FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id LEFT JOIN shipments AS s ON o.order_id = s.order_id";
        String expected = "SELECT o.*, p.name, p.description, s.shipment_id, s.origin, s.destination, s.is_arrived FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id LEFT JOIN shipments AS s ON o.order_id = s.order_id WHERE o.region = 'beijing' AND p.name = 'hammer' AND s.is_arrived = FALSE";

        testRowLevelFilter(FIRST_USER, inputSql, expected);

        // delete permission
        context.getRowLevelPermissions().remove(FIRST_USER, PRODUCTS_TABLE);
        context.getRowLevelPermissions().remove(FIRST_USER, SHIPMENTS_TABLE);
    }


    private void testRowLevelFilter(String username, String inputSql, String expectedSql) {
        String resultSql = context.addRowLevelFilter(username, inputSql);
        // replace \n with spaces and remove single apostrophes
        resultSql = resultSql.replace("\n", " ").replace("`", "");

        LOG.info("Input  SQL: {}", inputSql);
        LOG.info("Result SQL: {}", resultSql);
        assertEquals(expectedSql, resultSql);
    }

    /**
     * Create mysql cdc table products
     */
    private static void createTableOfProducts() {
        context.execute("DROP TABLE IF EXISTS " + PRODUCTS_TABLE);

        context.execute("CREATE TABLE IF NOT EXISTS " + PRODUCTS_TABLE + " (" +
                "       id                  INT PRIMARY KEY NOT ENFORCED ," +
                "       name                STRING                       ," +
                "       description         STRING                        " +
                ") WITH ( " +
                "       'connector' = 'mysql-cdc'            ," +
                "       'hostname'  = '192.168.90.150'       ," +
                "       'port'      = '3306'                 ," +
                "       'username'  = 'root'                 ," +
                "       'password'  = 'root@123456'          ," +
                "       'server-time-zone' = 'Asia/Shanghai' ," +
                "       'database-name' = 'demo'             ," +
                "       'table-name'    = '" + PRODUCTS_TABLE + "' " +
                ")"
        );
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
                "       order_status        BOOLEAN                      ," +
                "       region              STRING                        " +
                ") WITH ( " +
                "       'connector' = 'mysql-cdc'            ," +
                "       'hostname'  = '192.168.90.150'       ," +
                "       'port'      = '3306'                 ," +
                "       'username'  = 'root'                 ," +
                "       'password'  = 'root@123456'          ," +
                "       'server-time-zone' = 'Asia/Shanghai' ," +
                "       'database-name' = 'demo'             ," +
                "       'table-name'    = '" + ORDERS_TABLE + "' " +
                ")"
        );
    }

    /**
     * Create mysql cdc table shipments
     */
    private static void createTableOfShipments() {
        context.execute("DROP TABLE IF EXISTS " + SHIPMENTS_TABLE);

        context.execute("CREATE TABLE IF NOT EXISTS " + SHIPMENTS_TABLE + " (" +
                "       shipment_id          INT PRIMARY KEY NOT ENFORCED ," +
                "       order_id             INT                          ," +
                "       origin               STRING                       ," +
                "       destination          STRING                       ," +
                "       is_arrived           BOOLEAN                       " +
                ") WITH ( " +
                "       'connector' = 'mysql-cdc'            ," +
                "       'hostname'  = '192.168.90.150'       ," +
                "       'port'      = '3306'                 ," +
                "       'username'  = 'root'                 ," +
                "       'password'  = 'root@123456'          ," +
                "       'server-time-zone' = 'Asia/Shanghai' ," +
                "       'database-name' = 'demo'             ," +
                "       'table-name'    = '" + SHIPMENTS_TABLE + "' " +
                ")"
        );
    }
}
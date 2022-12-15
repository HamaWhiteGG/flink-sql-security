package com.dtwave.flink.security.parser;

import com.dtwave.flink.security.basic.AbstractBasicTest;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * @description: Rewrite SQL based on row-level filter conditions
 * @author: baisong
 * @version: 1.0.0
 * @date: 2022/12/10 12:24 PM
 */
public class ParserTest extends AbstractBasicTest {

    private static final Logger LOG = LoggerFactory.getLogger(ParserTest.class);


    @BeforeClass
    public static void init() {
        // create mysql cdc table orders
        createTableOfOrders();

        // create mysql cdc table products
        createTableOfProducts();

        // create mysql cdc table shipments
        createTableOfShipments();

        // create print sink table print_sink
        createTableOfPrintSink();
    }


    /**
     * Only select, no where clause
     */
    @Test
    public void testSelect() {
        String inputSql = "SELECT * FROM orders";
        String expected = "SELECT * FROM orders WHERE region = 'beijing'";

        testRowFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * Different users configure different permission points
     */
    @Test
    public void testSelectDiffUser() {
        String inputSql = "SELECT * FROM orders";
        String firstExpected = "SELECT * FROM orders WHERE region = 'beijing'";
        String secondExpected = "SELECT * FROM orders WHERE region = 'hangzhou'";

        testRowFilter(FIRST_USER, inputSql, firstExpected);
        testRowFilter(SECOND_USER, inputSql, secondExpected);
    }


    /**
     * Where there is a condition
     */
    @Test
    public void testSelectWhere() {
        String inputSql = "SELECT * FROM orders WHERE price > 45.0";
        String expected = "SELECT * FROM orders WHERE price > 45.0 AND region = 'beijing'";

        testRowFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * Where there is complex condition, add a pair of parentheses to the existing multiple where
     * conditions
     */
    @Test
    public void testSelectComplexWhere() {
        String inputSql = "SELECT * FROM orders WHERE price > 45.0 OR customer_name = 'John'";
        String expected = "SELECT * FROM orders WHERE (price > 45.0 OR customer_name = 'John') AND region = 'beijing'";

        testRowFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * With group by clause
     */
    @Test
    public void testSelectWhereGroupBy() {
        String inputSql = "SELECT customer_name, count(*) AS cnt FROM orders WHERE price > 45.0 GROUP BY customer_name";
        String expected = "SELECT customer_name, COUNT(*) AS cnt FROM orders WHERE price > 45.0 AND region = 'beijing' GROUP BY customer_name";

        testRowFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * The two tables of products and orders are left joined
     */
    @Test
    public void testJoin() {
        String inputSql = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id";
        String expected = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE o.region = 'beijing'";

        testRowFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * The two tables of products and orders are left joined, but without alias
     */
    @Test
    public void testJoinWithoutAlias() {
        String inputSql = "SELECT o.*, p.name, p.description FROM orders LEFT JOIN products ON orders.product_id = products.id";
        String expected = "SELECT o.*, p.name, p.description FROM orders LEFT JOIN products ON orders.product_id = products.id WHERE orders.region = 'beijing'";

        testRowFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * The two tables of products and orders are left joined, and there is a condition
     */
    @Test
    public void testJoinWhere() {
        String inputSql = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE o.price > 45.0 OR o.customer_name = 'John'";
        String expected = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE (o.price > 45.0 OR o.customer_name = 'John') AND o.region = 'beijing'";

        testRowFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * The products and orders two tables are left joined, and the left table comes from a subquery
     */
    @Test
    public void testJoinSubQueryWhere() {
        String inputSql = "SELECT o.*, p.name, p.description FROM (SELECT * FROM orders WHERE order_status = false) AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE o.price > 45.0 OR o.customer_name = 'John'";
        String expected = "SELECT o.*, p.name, p.description FROM (SELECT * FROM orders WHERE order_status = FALSE AND region = 'beijing') AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE o.price > 45.0 OR o.customer_name = 'John'";
        testRowFilter(FIRST_USER, inputSql, expected);
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
        testRowFilter(FIRST_USER, inputSql, expected);

        // delete permission
        context.getRowLevelPermissions().remove(FIRST_USER, PRODUCTS_TABLE);
    }


    /**
     * The order table order, the product table products, and the logistics information table
     * shipments are associated with the three tables
     */
    @Test
    public void testThreeJoin() {
        // add permission
        context.getRowLevelPermissions().put(FIRST_USER, PRODUCTS_TABLE, "name = 'hammer'");
        context.getRowLevelPermissions().put(FIRST_USER, SHIPMENTS_TABLE, "is_arrived = false");

        String inputSql = "SELECT o.*, p.name, p.description, s.shipment_id, s.origin, s.destination, s.is_arrived FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id LEFT JOIN shipments AS s ON o.order_id = s.order_id";
        String expected = "SELECT o.*, p.name, p.description, s.shipment_id, s.origin, s.destination, s.is_arrived FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id LEFT JOIN shipments AS s ON o.order_id = s.order_id WHERE o.region = 'beijing' AND p.name = 'hammer' AND s.is_arrived = FALSE";

        testRowFilter(FIRST_USER, inputSql, expected);

        // delete permission
        context.getRowLevelPermissions().remove(FIRST_USER, PRODUCTS_TABLE);
        context.getRowLevelPermissions().remove(FIRST_USER, SHIPMENTS_TABLE);
    }


    /**
     * insert-select.
     *
     * insert into print table from mysql cdc stream table.
     */
    @Test
    public void testInsertSelect() {
        String inputSql = "INSERT INTO print_sink SELECT * FROM orders";
        // the following () is what Calcite would automatically add
        String expected = "INSERT INTO print_sink (SELECT * FROM orders WHERE region = 'beijing')";

        testRowFilter(FIRST_USER, inputSql, expected);
    }


    /**
     * insert-select-select.
     *
     * insert into print table from mysql cdc stream table.
     */
    @Test
    public void testInsertSelectSelect() {
        String inputSql = "INSERT INTO print_sink SELECT * FROM (SELECT * FROM orders)";
        // the following () is what Calcite would automatically add
        String expected = "INSERT INTO print_sink (SELECT * FROM (SELECT * FROM orders WHERE region = 'beijing'))";

        testRowFilter(FIRST_USER, inputSql, expected);
    }


    private void testRowFilter(String username, String inputSql, String expectedSql) {
        String resultSql = context.addRowFilter(username, inputSql);
        // replace \n with spaces and remove single apostrophes
        resultSql = resultSql.replace("\n", " ").replace("`", "");

        LOG.info("Input  SQL: {}", inputSql);
        LOG.info("Result SQL: {}\n", resultSql);
        assertEquals(expectedSql, resultSql);
    }

}
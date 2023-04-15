package com.hw.security.flink.rewritten;

import com.hw.security.flink.basic.AbstractBasicTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * @description: DataMaskTest
 * @author: HamaWhite
 */
public class DataMaskTest extends AbstractBasicTest {

    private static final Logger LOG = LoggerFactory.getLogger(DataMaskTest.class);

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

        // add data mask policies
        policyManager.addPolicy(dataMaskPolicy(USER_A, TABLE_ORDERS, "price","MASK_HASH"));
    }


    /**
     * Only select, no where clause
     */
    @Test
    public void testSelect() {
        String inputSql = "SELECT * FROM orders";
        String expected = "SELECT * FROM orders WHERE region = 'beijing'";

        testApplyDataMask(USER_A, inputSql, expected);
    }

    /**
     * Only select with alias, no where clause
     */
    @Test
    public void testSelectWithAlias() {
//        String inputSql = "SELECT o.order_id, order_date FROM orders as o";
        String expected = "SELECT o.order_id, order_date FROM (select order_id, order_date, cast(ABS(product_id) as INT) as product_id FROM orders) o";
//
//        testApplyDataMask(USER_A, inputSql, expected);
        securityContext.applyDataMask(USER_A,expected);
    }

    /**
     * The two tables of products and orders are left joined
     */
    @Test
    public void testJoin() {
        String inputSql = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id";
//        String expected = "SELECT o.*, p.name, p.description FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id WHERE o.region = 'beijing'";

        securityContext.applyDataMask(USER_A,inputSql);
    }



    private void testApplyDataMask(String username, String inputSql, String expectedSql) {
        String resultSql = securityContext.applyDataMask(username, inputSql);
        // replace \n with spaces and remove single apostrophes
        resultSql = resultSql.replace("\n", " ").replace("`", "");

        LOG.info("Input  SQL: {}", inputSql);
        LOG.info("Result SQL: {}\n", resultSql);
        assertEquals(expectedSql, resultSql);
    }
}

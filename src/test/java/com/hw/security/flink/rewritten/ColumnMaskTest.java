package com.hw.security.flink.rewritten;

import com.hw.security.flink.basic.AbstractBasicTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * @description: ColumnMaskTest
 * @author: HamaWhite
 */
public class ColumnMaskTest extends AbstractBasicTest {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnMaskTest.class);

    @BeforeClass
    public static void createTable() {
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

        testColumnMasking(USER_A, inputSql, expected);
    }

    private void testColumnMasking(String username, String inputSql, String expectedSql) {
        String resultSql = context.addColumnMasking(username, inputSql);
        // replace \n with spaces and remove single apostrophes
        resultSql = resultSql.replace("\n", " ").replace("`", "");

        LOG.info("Input  SQL: {}", inputSql);
        LOG.info("Result SQL: {}\n", resultSql);
        assertEquals(expectedSql, resultSql);
    }
}

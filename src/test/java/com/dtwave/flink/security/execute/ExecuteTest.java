package com.dtwave.flink.security.execute;

import com.dtwave.flink.security.basic.AbstractBasicTest;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @description: Execute SQL based on row-level filter conditions
 * @author: baisong
 * @version: 1.0.0
 * @date: 2022/12/14 6:00 PM
 */
public class ExecuteTest extends AbstractBasicTest {

    @BeforeClass
    public static void init() {
        // create mysql cdc table orders
        createTableOfOrders();

        // create print sink table print_sink
        createTableOfPrintSink();
    }


    @Test
    public void testExecuteByFirstUser() {
        context.execute(FIRST_USER, "SELECT * FROM orders").print();
    }

    @Test
    public void testExecuteBySecondUser() {
        context.execute(SECOND_USER, "SELECT * FROM orders").print();
    }
}

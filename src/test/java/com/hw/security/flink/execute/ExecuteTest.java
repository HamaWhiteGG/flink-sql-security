package com.hw.security.flink.execute;

import com.hw.security.flink.basic.AbstractBasicTest;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Note: When running manually, first temporarily comment out the @Ignore annotation on the class,
 * and then optimize it in the next step
 *
 * @description: Execute SQL based on row-level filter conditions
 * @author: HamaWhite
 */
@Ignore
public class ExecuteTest extends AbstractBasicTest {


    @BeforeClass
    public static void init() {
        // create mysql cdc table orders
        createTableOfOrders();
    }

    /**
     * Execute without row-level filter
     */
    @Test
    public void testExecute() {
        context.execute("SELECT * FROM orders").print();
    }


    /**
     * Execute with the first user's row-level filter
     */
    @Test
    public void testExecuteByFirstUser() {
        context.execute(FIRST_USER, "SELECT * FROM orders").print();
    }

    /**
     * Execute with the second user's row-level filter
     */
    @Test
    public void testExecuteBySecondUser() {
        context.execute(SECOND_USER, "SELECT * FROM orders").print();
    }
}

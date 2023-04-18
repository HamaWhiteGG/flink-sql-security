package com.hw.security.flink.execute;

import com.hw.security.flink.basic.AbstractBasicTest;
import org.apache.flink.types.Row;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Note: When running manually, first temporarily comment out the @Ignore annotation on the class,
 * and then optimize it in the next step
 *
 * @description: Execute SQL based on row-level filter conditions
 * @author: HamaWhite
 */
@Ignore
public class ExecuteRowFilterTest extends AbstractBasicTest {

    @BeforeClass
    public static void init() {
        // create mysql cdc table orders
        createTableOfOrders();

        // add row filter policies
        policyManager.addPolicy(rowFilterPolicy(USER_A, TABLE_ORDERS, "region = 'beijing'"));
        policyManager.addPolicy(rowFilterPolicy(USER_B, TABLE_ORDERS, "region = 'hangzhou'"));
    }

    /**
     * Execute without row-level filter
     */
    @Test
    public void testExecuteWithoutRowFilter() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        Object[][] expected = {
                {10001, "Jack", 102, "beijing"},
                {10002, "Sally", 105, "beijing"},
                {10003, "Edward", 106, "hangzhou"},
                {10004, "John", 103, "hangzhou"},
                {10005, "Edward", 104, "shanghai"},
                {10006, "Jack", 103, "shanghai"}
        };
        List<Row> rowList = securityContext.execute(sql, expected.length);
        assertExecuteResult(expected, rowList);
    }


    /**
     * User A can only view data in the beijing region
     */
    @Test
    public void testExecuteByUserA() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        Object[][] expected = {
                {10001, "Jack", 102, "beijing"},
                {10002, "Sally", 105, "beijing"}
        };
        executeRowFilter(USER_A, sql, expected);
    }

    /**
     * User B can only view data in the hangzhou region
     */
    @Test
    public void testExecuteByUserB() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        Object[][] expected = {
                {10003, "Edward", 106, "hangzhou"},
                {10004, "John", 103, "hangzhou"}
        };
        executeRowFilter(USER_B, sql, expected);
    }

    private void executeRowFilter(String username, String sql, Object[][] expected) {
        List<Row> rowList = securityContext.execute(username, sql, expected.length);
        assertExecuteResult(expected, rowList);
    }

    private void assertExecuteResult(Object[][] expectedArray, List<Row> actualList) {
        Object[][] actualArray = actualList.stream()
                .map(e -> {
                    Object[] array = new Object[e.getArity()];
                    for (int pos = 0; pos < e.getArity(); pos++) {
                        array[pos] = e.getField(pos);
                    }
                    return array;
                }).collect(Collectors.toList())
                .toArray(new Object[0][0]);

        assertThat(actualArray).isEqualTo(expectedArray);
    }
}

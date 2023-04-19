package com.hw.security.flink.rewrite;

import com.hw.security.flink.basic.AbstractBasicTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Rewrite SQL based on data mask conditions
 *
 * @author: HamaWhite
 */
public class RewriteDataMaskTest extends AbstractBasicTest {

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
     * Only select
     */
    @Test
    public void testSelect() {
        String sql = "SELECT * FROM orders";
        
        String expected = "SELECT                   " +
                "       *                           " +
                "FROM (                             " +
                "       SELECT                      " +
                "               order_id           ," +
                "               order_date         ," +
                "               customer_name      ," +
                "               product_id         ," +
                "               CAST(mask_hash(price) AS DECIMAL(10, 1)) AS price,  " +
                "               order_status       ," +
                "               region              " +
                "       FROM                        " +
                "               orders              " +
                "     ) AS orders                   ";

        rewriteDataMask(USER_A, sql, expected);
    }


    /**
     * Only select with alias
     */
    @Test
    public void testSelectWithAlias() {
        String sql = "SELECT o.* FROM orders as o";

        String expected = "SELECT                   " +
                "       o.*                         " +
                "FROM (                             " +
                "       SELECT                      " +
                "               order_id           ," +
                "               order_date         ," +
                "               customer_name      ," +
                "               product_id         ," +
                "               CAST(mask_hash(price) AS DECIMAL(10, 1)) AS price,  " +
                "               order_status       ," +
                "               region              " +
                "       FROM                        " +
                "               orders              " +
                "     ) AS o                        ";
        
        rewriteDataMask(USER_A, sql, expected);
    }

    /**
     * The two tables of products and orders are left joined.
     * <p> products have an alias p, order has no alias
     */
    @Test
    public void testJoin() {
        String sql = "SELECT                        " +
                "       orders.*                   ," +
                "       p.name                     ," +
                "       p.description               " +
                "FROM                               " +
                "       orders                      " +
                "LEFT JOIN                          " +
                "       products AS p               " +
                "ON                                 " +
                "       orders.product_id = p.id    ";

        String expected = "SELECT                   " +
                "       orders.*                   ," +
                "       p.name                     ," +
                "       p.description               " +
                "FROM (                             " +
                "       SELECT                      " +
                "               order_id           ," +
                "               order_date         ," +
                "               customer_name      ," +
                "               product_id         ," +
                "               CAST(mask_hash(price) AS DECIMAL(10, 1)) AS price, " +
                "               order_status       ," +
                "               region              " +
                "       FROM                        " +
                "               orders              " +
                "     ) AS orders                   " +
                "LEFT JOIN                          " +
                "       products AS p               " +
                "ON                                 " +
                "       orders.product_id = p.id    ";

        rewriteDataMask(USER_A, sql, expected);
    }
}

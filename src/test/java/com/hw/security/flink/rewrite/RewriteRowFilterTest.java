/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hw.security.flink.rewrite;

import com.hw.security.flink.basic.AbstractBasicTest;
import com.hw.security.flink.policy.RowFilterPolicy;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Rewrite SQL based on row filter conditions
 *
 * @author: HamaWhite
 */
public class RewriteRowFilterTest extends AbstractBasicTest {

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

        // add row filter policies
        policyManager.addPolicy(rowFilterPolicy(USER_A, TABLE_ORDERS, "region = 'beijing'"));
        policyManager.addPolicy(rowFilterPolicy(USER_B, TABLE_ORDERS, "region = 'hangzhou'"));
    }

    /**
     * Only select
     */
    @Test
    public void testSelect() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        String expected = "SELECT                           " +
                "       orders.order_id                    ," +
                "       orders.customer_name               ," +
                "       orders.product_id                  ," +
                "       orders.region                       " +
                "FROM                                       " +
                "       hive.default.orders AS orders       " +
                "WHERE                                      " +
                "       orders.region = 'beijing'           ";

        rewriteRowFilter(USER_A, sql, expected);
    }

    /**
     * Different users configure different policies
     */
    @Test
    public void testSelectDiffUser() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        String expectedUserA = "SELECT                      " +
                "       orders.order_id                    ," +
                "       orders.customer_name               ," +
                "       orders.product_id                  ," +
                "       orders.region                       " +
                "FROM                                       " +
                "       hive.default.orders AS orders       " +
                "WHERE                                      " +
                "       orders.region = 'beijing'           ";

        String expectedUserB = "SELECT                       " +
                "       orders.order_id                     ," +
                "       orders.customer_name                ," +
                "       orders.product_id                   ," +
                "       orders.region                        " +
                "FROM                                        " +
                "       hive.default.orders AS orders        " +
                "WHERE                                       " +
                "       orders.region = 'hangzhou'           ";

        rewriteRowFilter(USER_A, sql, expectedUserA);
        rewriteRowFilter(USER_B, sql, expectedUserB);
    }

    /**
     * Where there is a condition
     */
    @Test
    public void testSelectWhere() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders WHERE price > 45.0";

        String expected = "SELECT                           " +
                "       orders.order_id                    ," +
                "       orders.customer_name               ," +
                "       orders.product_id                  ," +
                "       orders.region                       " +
                "FROM                                       " +
                "       hive.default.orders AS orders       " +
                "WHERE                                      " +
                "       orders.price > 45.0                 " +
                "       AND orders.region = 'beijing'       ";

        rewriteRowFilter(USER_A, sql, expected);
    }

    /**
     * Where there is complex condition, add a pair of parentheses to the existing multiple where
     * conditions
     */
    @Test
    public void testSelectComplexWhere() {
        String sql = "SELECT                                " +
                "       order_id                           ," +
                "       customer_name                      ," +
                "       product_id                         ," +
                "       region                              " +
                "FROM                                       " +
                "       orders                              " +
                "WHERE                                      " +
                "       price > 45.0                        " +
                "       OR customer_name = 'John'           ";

        String expected = "SELECT                                               " +
                "       orders.order_id                                        ," +
                "       orders.customer_name                                   ," +
                "       orders.product_id                                      ," +
                "       orders.region                                           " +
                "FROM                                                           " +
                "       hive.default.orders AS orders                           " +
                "WHERE                                                          " +
                "       (orders.price > 45.0 OR orders.customer_name = 'John')  " +
                "       AND orders.region = 'beijing'                           ";

        rewriteRowFilter(USER_A, sql, expected);
    }

    /**
     * With group by clause
     */
    @Test
    public void testSelectWhereGroupBy() {
        String sql = "SELECT                        " +
                "       customer_name              ," +
                "       count(*) AS cnt             " +
                "FROM                               " +
                "       orders                      " +
                "WHERE                              " +
                "       price > 45.0                " +
                "GROUP BY                           " +
                "       customer_name               ";

        String expected = "SELECT                           " +
                "       orders.customer_name               ," +
                "       COUNT(*) AS cnt                     " +
                "FROM                                       " +
                "       hive.default.orders AS orders       " +
                "WHERE                                      " +
                "       orders.price > 45.0                 " +
                "       AND orders.region = 'beijing'       " +
                "GROUP BY                                   " +
                "       orders.customer_name                ";

        rewriteRowFilter(USER_A, sql, expected);
    }

    /**
     * The two tables of products and orders are left joined
     */
    @Test
    public void testJoin() {
        String sql = "SELECT                        " +
                "       o.order_id                 ," +
                "       o.customer_name            ," +
                "       o.product_id               ," +
                "       o.region                   ," +
                "       p.name                     ," +
                "       p.description               " +
                "FROM                               " +
                "       orders AS o                 " +
                "LEFT JOIN                          " +
                "       products AS p               " +
                "ON                                 " +
                "       o.product_id = p.id         ";

        String expected = "SELECT                           " +
                "       o.order_id                         ," +
                "       o.customer_name                    ," +
                "       o.product_id                       ," +
                "       o.region                           ," +
                "       p.name                             ," +
                "       p.description                       " +
                "FROM                                       " +
                "       hive.default.orders AS o            " +
                "LEFT JOIN                                  " +
                "       hive.default.products AS p          " +
                "ON                                         " +
                "       o.product_id = p.id                 " +
                "WHERE                                      " +
                "       o.region = 'beijing'                ";

        rewriteRowFilter(USER_A, sql, expected);
    }

    /**
     * The two tables of products and orders are left joined, but without alias
     */
    @Test
    public void testJoinWithoutAlias() {
        String sql = "SELECT                            " +
                "       orders.order_id                ," +
                "       orders.customer_name           ," +
                "       orders.product_id              ," +
                "       orders.region                  ," +
                "       products.name                  ," +
                "       products.description            " +
                "FROM                                   " +
                "       orders                          " +
                "LEFT JOIN                              " +
                "       products                        " +
                "ON                                     " +
                "       orders.product_id = products.id ";

        String expected = "SELECT                           " +
                "       orders.order_id                    ," +
                "       orders.customer_name               ," +
                "       orders.product_id                  ," +
                "       orders.region                      ," +
                "       products.name                      ," +
                "       products.description                " +
                "FROM                                       " +
                "       hive.default.orders AS orders       " +
                "LEFT JOIN                                  " +
                "       hive.default.products AS products   " +
                "ON                                         " +
                "       orders.product_id = products.id     " +
                "WHERE                                      " +
                "       orders.region = 'beijing'           ";

        rewriteRowFilter(USER_A, sql, expected);
    }

    /**
     * The two tables of products and orders are left joined, and there is a condition
     */
    @Test
    public void testJoinWhere() {
        String sql = "SELECT                            " +
                "       o.order_id                     ," +
                "       o.customer_name                ," +
                "       o.product_id                   ," +
                "       o.region                       ," +
                "       p.name                         ," +
                "       p.description                   " +
                "FROM                                   " +
                "       orders AS o                     " +
                "LEFT JOIN                              " +
                "       products AS p                   " +
                "ON                                     " +
                "       o.product_id = p.id             " +
                "WHERE                                  " +
                "       o.price > 45.0                  " +
                "       OR o.customer_name = 'John'     ";

        String expected = "SELECT                                       " +
                "       o.order_id                                     ," +
                "       o.customer_name                                ," +
                "       o.product_id                                   ," +
                "       o.region                                       ," +
                "       p.name                                         ," +
                "       p.description                                   " +
                "FROM                                                   " +
                "       hive.default.orders AS o                        " +
                "LEFT JOIN                                              " +
                "       hive.default.products AS p                      " +
                "ON                                                     " +
                "       o.product_id = p.id                             " +
                "WHERE                                                  " +
                "       (o.price > 45.0 OR o.customer_name = 'John')    " +
                "       AND o.region = 'beijing'                        ";

        rewriteRowFilter(USER_A, sql, expected);
    }

    /**
     * The products and orders two tables are left joined, and the left table comes from a sub-query
     */
    @Test
    public void testJoinSubQueryWhere() {
        String sql = "SELECT                            " +
                "       o.order_id                     ," +
                "       o.customer_name                ," +
                "       o.product_id                   ," +
                "       o.region                       ," +
                "       p.name                         ," +
                "       p.description                   " +
                "FROM (                                 " +
                "       SELECT                          " +
                "               order_id               ," +
                "               customer_name          ," +
                "               price                  ," +
                "               product_id             ," +
                "               region                  " +
                "       FROM                            " +
                "               orders                  " +
                "       WHERE order_status = FALSE      " +
                "     ) AS o                            " +
                "LEFT JOIN                              " +
                "       products AS p                   " +
                "ON                                     " +
                "       o.product_id = p.id             " +
                "WHERE                                  " +
                "       o.price > 45.0                  " +
                "       OR o.customer_name = 'John'     ";

        String expected = "SELECT                               " +
                "       o.order_id                             ," +
                "       o.customer_name                        ," +
                "       o.product_id                           ," +
                "       o.region                               ," +
                "       p.name                                 ," +
                "       p.description                           " +
                "FROM (                                         " +
                "       SELECT                                  " +
                "               orders.order_id                ," +
                "               orders.customer_name           ," +
                "               orders.price                   ," +
                "               orders.product_id              ," +
                "               orders.region                   " +
                "       FROM                                    " +
                "               hive.default.orders AS orders   " +
                "       WHERE                                   " +
                "           orders.order_status = FALSE         " +
                "           AND orders.region = 'beijing'       " +
                "     ) AS o                                    " +
                "LEFT JOIN                                      " +
                "       hive.default.products AS p              " +
                "ON                                             " +
                "       o.product_id = p.id                     " +
                "WHERE                                          " +
                "       o.price > 45.0                          " +
                "       OR o.customer_name = 'John'             ";

        rewriteRowFilter(USER_A, sql, expected);
    }

    /**
     * The two tables of orders and products are joined, and both have row-level filter conditions
     */
    @Test
    public void testJoinWithBothPermissions() {
        RowFilterPolicy policy = rowFilterPolicy(USER_A, TABLE_PRODUCTS, "name = 'hammer'");
        // add policy
        policyManager.addPolicy(policy);

        String sql = "SELECT                        " +
                "       o.order_id                 ," +
                "       o.customer_name            ," +
                "       o.product_id               ," +
                "       o.region                   ," +
                "       p.name                     ," +
                "       p.description               " +
                "FROM                               " +
                "       orders AS o                 " +
                "LEFT JOIN                          " +
                "       products AS p               " +
                "ON                                 " +
                "       o.product_id = p.id         ";

        String expected = "SELECT                    " +
                "       o.order_id                  ," +
                "       o.customer_name             ," +
                "       o.product_id                ," +
                "       o.region                    ," +
                "       p.name                      ," +
                "       p.description                " +
                "FROM                                " +
                "       hive.default.orders AS o     " +
                "LEFT JOIN                           " +
                "       hive.default.products AS p   " +
                "ON                                  " +
                "       o.product_id = p.id          " +
                "WHERE                               " +
                "       o.region = 'beijing'         " +
                "       AND p.name = 'hammer'        ";

        rewriteRowFilter(USER_A, sql, expected);

        // remove policy
        policyManager.removePolicy(policy);
    }

    /**
     * The order table order, the product table products, and the logistics information table
     * shipments are associated with the three tables
     */
    @Test
    public void testThreeJoin() {
        RowFilterPolicy policy1 = rowFilterPolicy(USER_A, TABLE_PRODUCTS, "name = 'hammer'");
        RowFilterPolicy policy2 = rowFilterPolicy(USER_A, TABLE_SHIPMENTS, "is_arrived = FALSE");

        // add policies
        policyManager.addPolicy(policy1);
        policyManager.addPolicy(policy2);

        String sql = "SELECT                        " +
                "       o.order_id                 ," +
                "       o.customer_name            ," +
                "       o.product_id               ," +
                "       o.region                   ," +
                "       p.name                     ," +
                "       p.description              ," +
                "       s.shipment_id              ," +
                "       s.origin                   ," +
                "       s.destination              ," +
                "       s.is_arrived                " +
                "FROM                               " +
                "       orders AS o                 " +
                "LEFT JOIN                          " +
                "       products AS p               " +
                "ON                                 " +
                "       o.product_id = p.id         " +
                "LEFT JOIN                          " +
                "       shipments AS s              " +
                "ON                                 " +
                "       o.order_id = s.order_id     ";

        String expected = "SELECT                    " +
                "       o.order_id                  ," +
                "       o.customer_name             ," +
                "       o.product_id                ," +
                "       o.region                    ," +
                "       p.name                      ," +
                "       p.description               ," +
                "       s.shipment_id               ," +
                "       s.origin                    ," +
                "       s.destination               ," +
                "       s.is_arrived                 " +
                "FROM                                " +
                "       hive.default.orders AS o     " +
                "LEFT JOIN                           " +
                "       hive.default.products AS p   " +
                "ON                                  " +
                "       o.product_id = p.id          " +
                "LEFT JOIN                           " +
                "       hive.default.shipments AS s  " +
                "ON                                  " +
                "       o.order_id = s.order_id      " +
                "WHERE                               " +
                "       o.region = 'beijing'         " +
                "       AND p.name = 'hammer'        " +
                "       AND s.is_arrived = FALSE     ";

        rewriteRowFilter(USER_A, sql, expected);

        // remove policies
        policyManager.removePolicy(policy1);
        policyManager.removePolicy(policy2);
    }

    /**
     * insert-select.
     * insert into print table from mysql cdc stream table.
     */
    @Test
    public void testInsertSelect() {
        String sql = "INSERT INTO print_sink SELECT * FROM orders";

        // the following () is what Calcite would automatically add
        String expected = "INSERT INTO print_sink (                 " +
                "SELECT                                             " +
                "       orders.order_id                            ," +
                "       orders.order_date                          ," +
                "       orders.customer_name                       ," +
                "       orders.product_id                          ," +
                "       orders.price                               ," +
                "       orders.order_status                        ," +
                "       orders.region                               " +
                "FROM                                               " +
                "       hive.default.orders AS orders               " +
                "WHERE                                              " +
                "        orders.region = 'beijing'                  " +
                ")                                                  ";

        rewriteRowFilter(USER_A, sql, expected);
    }

    /**
     * insert-select-select.
     * insert into print table from mysql cdc stream table.
     */
    @Test
    public void testInsertSelectSelect() {
        String sql = "INSERT INTO print_sink SELECT * FROM (SELECT * FROM orders) AS o";

        // the following () is what Calcite would automatically add
        String expected = "INSERT INTO print_sink (                 " +
                "SELECT                                             " +
                "       o.order_id                                 ," +
                "       o.order_date                               ," +
                "       o.customer_name                            ," +
                "       o.product_id                               ," +
                "       o.price                                    ," +
                "       o.order_status                             ," +
                "       o.region                                    " +
                "FROM (                                             " +
                "       SELECT                                      " +
                "               orders.order_id                    ," +
                "               orders.order_date                  ," +
                "               orders.customer_name               ," +
                "               orders.product_id                  ," +
                "               orders.price                       ," +
                "               orders.order_status                ," +
                "               orders.region                       " +
                "       FROM                                        " +
                "               hive.default.orders AS orders       " +
                "       WHERE                                       " +
                "               orders.region = 'beijing'           " +
                "     ) AS o                                        " +
                ")                                                  ";

        rewriteRowFilter(USER_A, sql, expected);
    }
}
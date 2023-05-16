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
        policyManager.addPolicy(dataMaskPolicy(USER_A, TABLE_ORDERS, "customer_name", "MASK"));
        policyManager.addPolicy(dataMaskPolicy(USER_A, TABLE_PRODUCTS, "name", "MASK_SHOW_LAST_4"));
        policyManager.addPolicy(dataMaskPolicy(USER_B, TABLE_ORDERS, "customer_name", "MASK_SHOW_FIRST_4"));
    }

    /**
     * Only select
     */
    @Test
    public void testSelect() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        // the alias is equal to the table name orders
        String expected = "SELECT                   " +
                "       orders.order_id            ," +
                "       orders.customer_name       ," +
                "       orders.product_id          ," +
                "       orders.region               " +
                "FROM (                             " +
                "       SELECT                      " +
                "               order_id           ," +
                "               order_date         ," +
                "               CAST(mask(customer_name) AS STRING) AS customer_name ," +
                "               product_id         ," +
                "               price              ," +
                "               order_status       ," +
                "               region              " +
                "       FROM                        " +
                "               hive.default.orders " +
                "     ) AS orders                   ";

        rewriteDataMask(USER_A, sql, expected);
    }

    /**
     * Only select with alias
     */
    @Test
    public void testSelectWithAlias() {
        String sql = "SELECT o.order_id, o.customer_name, o.product_id, o.region FROM orders AS o";

        // the alias is equal to 'o'
        String expected = "SELECT                   " +
                "       o.order_id                 ," +
                "       o.customer_name            ," +
                "       o.product_id               ," +
                "       o.region                    " +
                "FROM (                             " +
                "       SELECT                      " +
                "               order_id           ," +
                "               order_date         ," +
                "               CAST(mask(customer_name) AS STRING) AS customer_name ," +
                "               product_id         ," +
                "               price              ," +
                "               order_status       ," +
                "               region              " +
                "       FROM                        " +
                "               hive.default.orders " +
                "     ) AS o                        ";

        rewriteDataMask(USER_A, sql, expected);
    }

    /**
     * Different users configure different policies
     */
    @Test
    public void testSelectDiffUser() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        String expectedUserA = "SELECT              " +
                "       orders.order_id            ," +
                "       orders.customer_name       ," +
                "       orders.product_id          ," +
                "       orders.region               " +
                "FROM (                             " +
                "       SELECT                      " +
                "               order_id           ," +
                "               order_date         ," +
                "               CAST(mask(customer_name) AS STRING) AS customer_name ," +
                "               product_id         ," +
                "               price              ," +
                "               order_status       ," +
                "               region              " +
                "       FROM                        " +
                "               hive.default.orders " +
                "     ) AS orders                   ";

        String expectedUserB = "SELECT               " +
                "       orders.order_id             ," +
                "       orders.customer_name        ," +
                "       orders.product_id           ," +
                "       orders.region                " +
                "FROM (                              " +
                "       SELECT                       " +
                "               order_id            ," +
                "               order_date          ," +
                "               CAST(mask_show_first_n(customer_name, 4, 'x', 'x', 'x', -1, '1') AS STRING) " +
                "                       AS customer_name                                                   ," +
                "               product_id          ," +
                "               price               ," +
                "               order_status        ," +
                "               region               " +
                "       FROM                         " +
                "               hive.default.orders  " +
                "     ) AS orders                    ";

        rewriteDataMask(USER_A, sql, expectedUserA);
        rewriteDataMask(USER_B, sql, expectedUserB);
    }

    /**
     * The two tables of products and orders are left joined.
     * <p> products have an alias p, order has no alias
     */
    @Test
    public void testJoin() {
        String sql = "SELECT                        " +
                "       orders.order_id            ," +
                "       orders.customer_name       ," +
                "       orders.product_id          ," +
                "       orders.region              ," +
                "       p.name                     ," +
                "       p.description               " +
                "FROM                               " +
                "       orders                      " +
                "LEFT JOIN                          " +
                "       products AS p               " +
                "ON                                 " +
                "       orders.product_id = p.id    ";

        String expected = "SELECT                       " +
                "       orders.order_id                ," +
                "       orders.customer_name           ," +
                "       orders.product_id              ," +
                "       orders.region                  ," +
                "       p.name                         ," +
                "       p.description                   " +
                "FROM (                                 " +
                "       SELECT                          " +
                "               order_id               ," +
                "               order_date             ," +
                "               CAST(mask(customer_name) AS STRING) AS customer_name ," +
                "               product_id             ," +
                "               price                  ," +
                "               order_status           ," +
                "               region                  " +
                "       FROM                            " +
                "               hive.default.orders     " +
                "     ) AS orders                       " +
                "LEFT JOIN (                            " +
                "       SELECT                          " +
                "               id                     ," +
                "               CAST(mask_show_last_n(name, 4, 'x', 'x', 'x', -1, '1') AS STRING) AS name, " +
                "               description             " +
                "       FROM                            " +
                "               hive.default.products   " +
                "       ) AS p                          " +
                "ON                                     " +
                "       orders.product_id = p.id        ";

        rewriteDataMask(USER_A, sql, expected);
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
                "       FROM (                                  " +
                "               SELECT                          " +
                "                       order_id               ," +
                "                       order_date             ," +
                "                       CAST(mask(customer_name) AS STRING) AS customer_name ," +
                "                       product_id             ," +
                "                       price                  ," +
                "                       order_status           ," +
                "                       region                  " +
                "               FROM                            " +
                "                       hive.default.orders     " +
                "            ) AS orders                        " +
                "       WHERE                                   " +
                "               orders.order_status = FALSE     " +
                "     ) AS o                                    " +
                "LEFT JOIN (                                    " +
                "       SELECT                                  " +
                "               id                             ," +
                "               CAST(mask_show_last_n(name, 4, 'x', 'x', 'x', -1, '1') AS STRING) AS name ," +
                "               description                     " +
                "       FROM                                    " +
                "           hive.default.products               " +
                "          ) AS p                               " +
                "ON                                             " +
                "       o.product_id = p.id                     " +
                "WHERE                                          " +
                "       o.price > 45.0                          " +
                "       OR o.customer_name = 'John'             ";

        rewriteDataMask(USER_A, sql, expected);
    }

    /**
     * The order table order, the product table products, and the logistics information table
     * shipments are associated with the three tables
     */
    @Test
    public void testThreeJoin() {
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

        String expected = "SELECT                       " +
                "       o.order_id                     ," +
                "       o.customer_name                ," +
                "       o.product_id                   ," +
                "       o.region                       ," +
                "       p.name                         ," +
                "       p.description                  ," +
                "       s.shipment_id                  ," +
                "       s.origin                       ," +
                "       s.destination                  ," +
                "       s.is_arrived                    " +
                "FROM (                                 " +
                "       SELECT                          " +
                "               order_id               ," +
                "               order_date             ," +
                "               CAST(mask(customer_name) AS STRING) AS customer_name ," +
                "               product_id             ," +
                "               price                  ," +
                "               order_status           ," +
                "               region                  " +
                "       FROM                            " +
                "           hive.default.orders         " +
                "     ) AS o                            " +
                "LEFT JOIN (                            " +
                "       SELECT                          " +
                "               id                     ," +
                "               CAST(mask_show_last_n(name, 4, 'x', 'x', 'x', -1, '1') AS STRING) AS name, " +
                "               description             " +
                "       FROM                            " +
                "               hive.default.products   " +
                "       ) AS p                          " +
                "ON                                     " +
                "       o.product_id = p.id             " +
                "LEFT JOIN                              " +
                "       hive.default.shipments AS s     " +
                "ON                                     " +
                "       o.order_id = s.order_id         ";

        rewriteDataMask(USER_A, sql, expected);
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
                "FROM (                                             " +
                "       SELECT                                      " +
                "               order_id                           ," +
                "               order_date                         ," +
                "               CAST(mask(customer_name) AS STRING) AS customer_name ," +
                "               product_id                         ," +
                "               price                              ," +
                "               order_status                       ," +
                "               region                              " +
                "       FROM                                        " +
                "           hive.default.orders                     " +
                "     ) AS orders                                   " +
                ")                                                  ";

        rewriteDataMask(USER_A, sql, expected);
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
                "       FROM (                                      " +
                "               SELECT                              " +
                "                       order_id                   ," +
                "                       order_date                 ," +
                "                       CAST(mask(customer_name) AS STRING) AS customer_name ," +
                "                       product_id                 ," +
                "                       price                      ," +
                "                       order_status               ," +
                "                       region                      " +
                "               FROM                                " +
                "                       hive.default.orders         " +
                "            ) AS orders                            " +
                "     ) AS o                                        " +
                ")                                                  ";

        rewriteDataMask(USER_A, sql, expected);
    }
}

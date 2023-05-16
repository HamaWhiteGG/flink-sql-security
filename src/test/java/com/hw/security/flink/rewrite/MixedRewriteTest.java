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
 * Add row-level filter and column masking, then return new SQL.
 *
 * @author: HamaWhite
 */
public class MixedRewriteTest extends AbstractBasicTest {

    @BeforeClass
    public static void init() {
        // create mysql cdc table orders
        createTableOfOrders();

        // create mysql cdc table products
        createTableOfProducts();

        // add row filter policy
        policyManager.addPolicy(rowFilterPolicy(USER_A, TABLE_ORDERS, "region = 'beijing'"));
        policyManager.addPolicy(rowFilterPolicy(USER_A, TABLE_PRODUCTS, "name = 'hammer'"));

        // add data mask policies
        policyManager.addPolicy(dataMaskPolicy(USER_A, TABLE_ORDERS, "customer_name", "MASK"));
        policyManager.addPolicy(dataMaskPolicy(USER_A, TABLE_PRODUCTS, "name", "MASK_SHOW_LAST_4"));
    }

    /**
     * Only select
     */
    @Test
    public void testSelect() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        // the alias is equal to the table name orders
        String expected = "SELECT                           " +
                "       orders.order_id                    ," +
                "       orders.customer_name               ," +
                "       orders.product_id                  ," +
                "       orders.region                       " +
                "FROM (                                     " +
                "       SELECT                              " +
                "               order_id                   ," +
                "               order_date                 ," +
                "               CAST(mask(customer_name) AS STRING) AS customer_name ," +
                "               product_id                 ," +
                "               price                      ," +
                "               order_status               ," +
                "               region                      " +
                "       FROM                                " +
                "                hive.default.orders        " +
                "     ) AS orders                           " +
                "WHERE                                      " +
                "       orders.region = 'beijing'           ";

        mixedRewrite(USER_A, sql, expected);
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

        String expected = "SELECT                           " +
                "       orders.order_id                    ," +
                "       orders.customer_name               ," +
                "       orders.product_id                  ," +
                "       orders.region                      ," +
                "       p.name                             ," +
                "       p.description                       " +
                "FROM (                                     " +
                "       SELECT                              " +
                "               order_id                   ," +
                "               order_date                 ," +
                "               CAST(mask(customer_name) AS STRING) AS customer_name ," +
                "               product_id                 ," +
                "               price                      ," +
                "               order_status               ," +
                "               region                      " +
                "       FROM                                " +
                "               hive.default.orders         " +
                "     ) AS orders                           " +
                "LEFT JOIN (                                " +
                "       SELECT                              " +
                "               id                         ," +
                "               CAST(mask_show_last_n(name, 4, 'x', 'x', 'x', -1, '1') AS STRING) AS name, " +
                "               description                 " +
                "       FROM                                " +
                "               hive.default.products       " +
                "       ) AS p                              " +
                "ON                                         " +
                "       orders.product_id = p.id            " +
                "WHERE                                      " +
                "       orders.region = 'beijing'           " +
                "       AND p.name = 'hammer'               ";

        mixedRewrite(USER_A, sql, expected);
    }
}

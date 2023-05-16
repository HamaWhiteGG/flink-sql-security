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

package com.hw.security.flink.execute;

import com.hw.security.flink.basic.AbstractBasicTest;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Execute SQL based on data mask.
 *
 * <p> Note: Depending on the external Mysql environment, you can run it manually.
 *
 * @author: HamaWhite
 */
@Ignore
public class ExecuteDataMaskTest extends AbstractBasicTest {

    @BeforeClass
    public static void init() {
        // create mysql cdc table orders
        createTableOfOrders();

        // create print sink table print_sink
        createTableOfPrintSink();

        // add data mask policies
        policyManager.addPolicy(dataMaskPolicy(USER_A, TABLE_ORDERS, "customer_name", "MASK"));
        policyManager.addPolicy(dataMaskPolicy(USER_B, TABLE_ORDERS, "customer_name", "MASK_SHOW_FIRST_4"));
    }

    /**
     * Execute without data mask
     */
    @Test
    public void testExecute() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        Object[][] expected = {
                {10001, "Jack", 102, "beijing"},
                {10002, "Sally", 105, "beijing"},
                {10003, "Edward", 106, "hangzhou"},
                {10004, "John", 103, "hangzhou"},
                {10005, "Edward", 104, "shanghai"},
                {10006, "Jack", 103, "shanghai"}
        };
        execute(sql, expected);
    }

    /**
     * User A view the customer_name after mask
     */
    @Test
    public void testExecuteByUserA() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        Object[][] expected = {
                {10001, "Xxxx", 102, "beijing"},
                {10002, "Xxxxx", 105, "beijing"},
                {10003, "Xxxxxx", 106, "hangzhou"},
                {10004, "Xxxx", 103, "hangzhou"},
                {10005, "Xxxxxx", 104, "shanghai"},
                {10006, "Xxxx", 103, "shanghai"}
        };
        executeDataMask(USER_A, sql, expected);
    }

    /**
     * User B view the customer_name after mask_show_first_4
     */
    @Test
    public void testExecuteByUserB() {
        String sql = "SELECT order_id, customer_name, product_id, region FROM orders";

        Object[][] expected = {
                {10001, "Jack", 102, "beijing"},
                {10002, "Sallx", 105, "beijing"},
                {10003, "Edwaxx", 106, "hangzhou"},
                {10004, "John", 103, "hangzhou"},
                {10005, "Edwaxx", 104, "shanghai"},
                {10006, "Jack", 103, "shanghai"}
        };
        executeDataMask(USER_B, sql, expected);
    }
}

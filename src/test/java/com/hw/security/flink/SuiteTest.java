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

package com.hw.security.flink;

import com.hw.security.flink.common.CommonTest;
import com.hw.security.flink.execute.ExecuteDataMaskTest;
import com.hw.security.flink.execute.ExecuteRowFilterTest;
import com.hw.security.flink.execute.MixedExecuteTest;
import com.hw.security.flink.rewrite.MixedRewriteTest;
import com.hw.security.flink.rewrite.RewriteDataMaskTest;
import com.hw.security.flink.rewrite.RewriteRowFilterTest;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Add the @Ignore annotation and run it manually
 *
 * @description: SuiteTest
 * @author: HamaWhite
 */
@Ignore
@RunWith(Suite.class)
@Suite.SuiteClasses({CommonTest.class,
        PolicyManagerTest.class,
        RewriteRowFilterTest.class,
        RewriteDataMaskTest.class,
        MixedRewriteTest.class,
        ExecuteRowFilterTest.class,
        ExecuteDataMaskTest.class,
        MixedExecuteTest.class})
public class SuiteTest {
    /*
     * The entry class of the test suite is just to organize the test classes together for testing, without any test
     * methods.
     */
}

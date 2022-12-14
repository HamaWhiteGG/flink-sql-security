package com.dtwave.flink.security;

import com.dtwave.flink.security.execute.ExecuteTest;
import com.dtwave.flink.security.parser.ParserTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @description: SuiteTest
 * @author: baisong
 * @version: 1.0.0
 * @date: 2022/11/24 5:49 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ParserTest.class
        , ExecuteTest.class})
public class SuiteTest {

    /**
     * The entry class of the test suite is just to organize the test classes together for testing,
     * without any test methods.
     */
}

package com.hw.security.flink;

import com.hw.security.flink.execute.ExecuteTest;
import com.hw.security.flink.rewritten.RowFilterTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @description: SuiteTest
 * @author: HamaWhite
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({RowFilterTest.class
        , ExecuteTest.class})
public class SuiteTest {

    /*
      The entry class of the test suite is just to organize the test classes together for testing,
      without any test methods.
     */
}

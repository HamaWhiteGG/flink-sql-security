package com.hw.security.flink;

import com.hw.security.flink.common.CommonTest;
import com.hw.security.flink.execute.ExecuteRowFilterTest;
import com.hw.security.flink.rewritten.RewrittenDataMaskTest;
import com.hw.security.flink.rewritten.RewrittenRowFilterTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @description: SuiteTest
 * @author: HamaWhite
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({CommonTest.class
        , PolicyManagerTest.class
        , RewrittenRowFilterTest.class
        , RewrittenDataMaskTest.class
        , ExecuteRowFilterTest.class})
public class SuiteTest {

    /*
      The entry class of the test suite is just to organize the test classes together for testing,
      without any test methods.
     */
}

package com.hw.security.flink;

import com.hw.security.flink.model.DataMaskType;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * @description: PolicyManagerTest
 * @author: HamaWhite
 */
public class PolicyManagerTest extends TestCase {

    private PolicyManager policyManager =new PolicyManager();

    @Test
    public void testGetDataMaskType() {
        DataMaskType ret = policyManager.getDataMaskType("MASK_HASH");

        assertThat(ret).isNotNull();
        assertThat(ret.getItemId()).isEqualTo(4L);
        assertThat(ret.getName()).isEqualTo("MASK_HASH");
        assertThat(ret.getLabel()).isEqualTo("Hash");
        assertThat(ret.getDescription()).isEqualTo("Hash the value");
        assertThat(ret.getTransformer()).isEqualTo("mask_hash({col})");
        assertThat(ret.getDataMaskOptions()).isEqualTo(Collections.emptyMap());
    }
}
package com.hw.security.flink.common;

import com.google.common.collect.ImmutableList;
import com.hw.security.flink.basic.AbstractBasicTest;
import com.hw.security.flink.model.ColumnEntity;
import com.hw.security.flink.model.TableEntity;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @description: CommonTest
 * @author: HamaWhite
 */
public class CommonTest extends AbstractBasicTest {

    private static final Logger LOG = LoggerFactory.getLogger(CommonTest.class);

    @BeforeClass
    public static void createTable() {
        // create mysql cdc table orders
        createTableOfOrders();
    }

    @Test
    public void testGetTable() {
        TableEntity actual = context.getTable(TABLE_ORDERS);
        List<ColumnEntity> columnList = ImmutableList.of(
                new ColumnEntity("order_id", "INT"),
                new ColumnEntity("order_date", "TIMESTAMP(0)"),
                new ColumnEntity("customer_name", "STRING"),
                new ColumnEntity("product_id", "INT"),
                new ColumnEntity("price", "DECIMAL(10, 5)"),
                new ColumnEntity("order_status", "BOOLEAN"),
                new ColumnEntity("region", "STRING")
        );
        TableEntity expected = new TableEntity(TABLE_ORDERS, columnList);
        assertEquals(expected, actual);
    }
}

package com.hw.security.flink.basic;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import com.hw.security.flink.SecurityContext;

/**
 * @description: AbstractBasicTest
 * @author: HamaWhite
 */
public abstract class AbstractBasicTest {

    protected static final SecurityContext context = SecurityContext.getInstance();

    // user A
    protected static final String FIRST_USER = "hamawhite";
    // user B
    protected static final String SECOND_USER = "song.bs";


    protected static final String ORDERS_TABLE = "orders";
    protected static final String PRODUCTS_TABLE = "products";
    protected static final String SHIPMENTS_TABLE = "shipments";


    public AbstractBasicTest() {
        // set row level permissions
        Table<String, String, String> rowLevelPermissions = HashBasedTable.create();
        rowLevelPermissions.put(FIRST_USER, ORDERS_TABLE, "region = 'beijing'");
        rowLevelPermissions.put(SECOND_USER, ORDERS_TABLE, "region = 'hangzhou'");
        context.setRowLevelPermissions(rowLevelPermissions);
    }


    /**
     * Create mysql cdc table products
     */
    protected static void createTableOfProducts() {
        context.execute("DROP TABLE IF EXISTS " + PRODUCTS_TABLE);

        context.execute("CREATE TABLE IF NOT EXISTS " + PRODUCTS_TABLE + " (" +
                "       id                  INT PRIMARY KEY NOT ENFORCED ," +
                "       name                STRING                       ," +
                "       description         STRING                        " +
                ") WITH ( " +
                "       'connector' = 'mysql-cdc'            ," +
                "       'hostname'  = '192.168.90.150'       ," +
                "       'port'      = '3306'                 ," +
                "       'username'  = 'root'                 ," +
                "       'password'  = 'root@123456'          ," +
                "       'server-time-zone' = 'Asia/Shanghai' ," +
                "       'database-name' = 'demo'             ," +
                "       'table-name'    = '" + PRODUCTS_TABLE + "' " +
                ")"
        );
    }


    /**
     * Create mysql cdc table orders
     */
    protected static void createTableOfOrders() {
        context.execute("DROP TABLE IF EXISTS " + ORDERS_TABLE);

        context.execute("CREATE TABLE IF NOT EXISTS " + ORDERS_TABLE + " (" +
                "       order_id            INT PRIMARY KEY NOT ENFORCED ," +
                "       order_date          TIMESTAMP(0)                 ," +
                "       customer_name       STRING                       ," +
                "       product_id          INT                          ," +
                "       price               DECIMAL(10, 5)               ," +
                "       order_status        BOOLEAN                      ," +
                "       region              STRING                        " +
                ") WITH ( " +
                "       'connector' = 'mysql-cdc'            ," +
                "       'hostname'  = '192.168.90.150'       ," +
                "       'port'      = '3306'                 ," +
                "       'username'  = 'root'                 ," +
                "       'password'  = 'root@123456'          ," +
                "       'server-time-zone' = 'Asia/Shanghai' ," +
                "       'database-name' = 'demo'             ," +
                "       'table-name'    = '" + ORDERS_TABLE + "' " +
                ")"
        );
    }


    /**
     * Create mysql cdc table print_sink
     */
    protected static void createTableOfPrintSink() {
        context.execute("DROP TABLE IF EXISTS print_sink ");

        context.execute("CREATE TABLE IF NOT EXISTS print_sink (" +
                "       order_id            INT PRIMARY KEY NOT ENFORCED ," +
                "       order_date          TIMESTAMP(0)                 ," +
                "       customer_name       STRING                       ," +
                "       product_id          INT                          ," +
                "       price               DECIMAL(10, 5)               ," +
                "       order_status        BOOLEAN                      ," +
                "       region              STRING                        " +
                ") WITH ( " +
                "       'connector' = 'print'            " +
                ")"
        );
    }


    /**
     * Create mysql cdc table shipments
     */
    protected static void createTableOfShipments() {
        context.execute("DROP TABLE IF EXISTS " + SHIPMENTS_TABLE);

        context.execute("CREATE TABLE IF NOT EXISTS " + SHIPMENTS_TABLE + " (" +
                "       shipment_id          INT PRIMARY KEY NOT ENFORCED ," +
                "       order_id             INT                          ," +
                "       origin               STRING                       ," +
                "       destination          STRING                       ," +
                "       is_arrived           BOOLEAN                       " +
                ") WITH ( " +
                "       'connector' = 'mysql-cdc'            ," +
                "       'hostname'  = '192.168.90.150'       ," +
                "       'port'      = '3306'                 ," +
                "       'username'  = 'root'                 ," +
                "       'password'  = 'root@123456'          ," +
                "       'server-time-zone' = 'Asia/Shanghai' ," +
                "       'database-name' = 'demo'             ," +
                "       'table-name'    = '" + SHIPMENTS_TABLE + "' " +
                ")"
        );
    }

}

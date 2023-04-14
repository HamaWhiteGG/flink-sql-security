package com.hw.security.flink.basic;

import com.hw.security.flink.PolicyManager;
import com.hw.security.flink.SecurityContext;
import com.hw.security.flink.policy.DataMaskPolicy;
import com.hw.security.flink.policy.RowFilterPolicy;
import org.junit.BeforeClass;

/**
 * @description: AbstractBasicTest
 * @author: HamaWhite
 */
public abstract class AbstractBasicTest {

    private static final String CATALOG_NAME = "default_catalog";
    private static final String DATABASE = "default_database";

    protected static final String USER_A = "user_A";
    protected static final String USER_B = "user_B";

    protected static final String TABLE_ORDERS = "orders";
    protected static final String TABLE_PRODUCTS = "products";
    protected static final String TABLE_SHIPMENTS = "shipments";

    protected static PolicyManager manager;
    protected static SecurityContext context;


    @BeforeClass
    public static void setup() {
        manager = new PolicyManager();
        context = new SecurityContext(manager);
    }

    public static RowFilterPolicy rowFilterPolicy(String username, String tableName, String condition) {
        return new RowFilterPolicy(username, CATALOG_NAME, DATABASE, tableName, condition);
    }

    public static DataMaskPolicy dataMaskPolicy(String username, String tableName, String columnName, String condition) {
        return new DataMaskPolicy(username, CATALOG_NAME, DATABASE, tableName, columnName, condition);
    }

    /**
     * Create mysql cdc table products
     */
    protected static void createTableOfProducts() {
        context.execute("DROP TABLE IF EXISTS " + TABLE_PRODUCTS);

        context.execute("CREATE TABLE IF NOT EXISTS " + TABLE_PRODUCTS + " (" +
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
                "       'table-name'    = '" + TABLE_PRODUCTS + "' " +
                ")"
        );
    }

    /**
     * Create mysql cdc table orders
     */
    protected static void createTableOfOrders() {
        context.execute("DROP TABLE IF EXISTS " + TABLE_ORDERS);

        context.execute("CREATE TABLE IF NOT EXISTS " + TABLE_ORDERS + " (" +
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
                "       'table-name'    = '" + TABLE_ORDERS + "' " +
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
        context.execute("DROP TABLE IF EXISTS " + TABLE_SHIPMENTS);

        context.execute("CREATE TABLE IF NOT EXISTS " + TABLE_SHIPMENTS + " (" +
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
                "       'table-name'    = '" + TABLE_SHIPMENTS + "' " +
                ")"
        );
    }
}

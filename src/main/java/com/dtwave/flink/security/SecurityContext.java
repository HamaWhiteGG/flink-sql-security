package com.dtwave.flink.security;

import com.google.common.collect.Table;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static com.dtwave.flink.security.Constant.EXECUTE_USERNAME;

/**
 * @description: SecurityContext
 * @author: baisong
 * @version: 1.0.0
 * @date: 2022/12/10 12:16 PM
 */
public class SecurityContext {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);

    private static volatile SecurityContext singleton;

    private final TableEnvironmentImpl tableEnv;
    private final Random rand = new Random();

    private Table<String, String, String> rowLevelPermissions;

    public static SecurityContext getInstance() {
        if (singleton == null) {
            synchronized (SecurityContext.class) {
                if (singleton == null) {
                    singleton = new SecurityContext();
                }
            }
        }
        return singleton;
    }


    private SecurityContext() {
        Configuration configuration = new Configuration();
        int port = randInt(8082, 8188);
        configuration.setInteger("rest.port", port);
        LOG.info("WebUI is http://127.0.0.1:{}", port);
        configuration.setBoolean("table.dynamic-table-options.enabled", true);

        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration)
                .enableCheckpointing(3 * 1000L)
                .setParallelism(1)) {
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();

            this.tableEnv = (TableEnvironmentImpl) StreamTableEnvironment.create(env, settings);
        } catch (Exception e) {
            LOG.error("init flink execution environment error: ", e);
            throw new FlinkRuntimeException(e);
        }
    }


    /**
     * Add row-level filter conditions and return new SQL
     */
    public String addRowFilter(String username, String singleSql) {
        System.setProperty(EXECUTE_USERNAME, username);

        // in the modified SqlSelect, filter conditions will be added to the where clause
        SqlNode parsedTree = tableEnv.getParser().parseSql(singleSql);
        return parsedTree.toString();
    }


    /**
     * Query the configured permission point according to the user name and table name, and return
     * it to SqlBasicCall
     */
    public SqlBasicCall queryPermissions(String username, String tableName) {
        String permissions = rowLevelPermissions.get(username, tableName);
        LOG.info("username: {}, tableName: {}, permissions: {}", username, tableName, permissions);
        if (permissions != null) {
            return (SqlBasicCall) tableEnv.getParser().parseExpression(permissions);
        }
        return null;
    }


    /**
     * Execute the single sql without user permissions
     */
    public TableResult execute(String singleSql) {
        LOG.info("Execute single sql: {}", singleSql);
        return tableEnv.executeSql(singleSql);
    }


    /**
     * Execute the single sql with user permissions
     */
    public TableResult execute(String username, String singleSql) {
        System.setProperty(EXECUTE_USERNAME, username);
        return tableEnv.executeSql(singleSql);
    }

    public Table<String, String, String> getRowLevelPermissions() {
        return rowLevelPermissions;
    }

    public void setRowLevelPermissions(Table<String, String, String> rowLevelPermissions) {
        this.rowLevelPermissions = rowLevelPermissions;
    }

    private int randInt(int min, int max) {
        return rand.nextInt((max - min) + 1) + min;
    }
}

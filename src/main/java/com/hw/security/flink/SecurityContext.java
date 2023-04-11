package com.hw.security.flink;

import com.google.common.collect.Table;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * @description: SecurityContext
 * @author: HamaWhite
 */
public class SecurityContext {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);

    private static SecurityContext singleton;

    private final TableEnvironmentImpl tableEnv;

    private final ParserImpl parser;

    private Table<String, String, String> rowLevelPermissions;

    public static synchronized SecurityContext getInstance() {
        if (singleton == null) {
            singleton = new SecurityContext();
        }
        return singleton;
    }


    private SecurityContext() {
        Configuration configuration = new Configuration();
        int port = new Random().nextInt((8188 - 8082) + 1) + 8082;
        configuration.setInteger("rest.port", port);
        LOG.info("WebUI is http://127.0.0.1:{}", port);
        configuration.setBoolean("table.dynamic-table-options.enabled", true);

        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration)) {
            env.enableCheckpointing(3 * 1000L).setParallelism(1);

            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();
            this.tableEnv = (TableEnvironmentImpl) StreamTableEnvironment.create(env, settings);
            this.parser = (ParserImpl) tableEnv.getParser();
        } catch (Exception e) {
            throw new FlinkRuntimeException("init local flink execution environment error", e);
        }
    }


    /**
     * Add row-level filter conditions and return new SQL
     */
    public String addRowFilter(String username, String singleSql) {
        System.setProperty(Constant.EXECUTE_USERNAME, username);

        // in the modified SqlSelect, filter conditions will be added to the where clause
        SqlNode parsedTree = parser.parseSql(singleSql);
        return parsedTree.toString();
    }


    /**
     * Query the configured permission point according to the username and table name, and return
     * it to SqlBasicCall
     */
    public SqlBasicCall queryPermissions(String username, String tableName) {
        String permissions = rowLevelPermissions.get(username, tableName);
        LOG.info("username: {}, tableName: {}, permissions: {}", username, tableName, permissions);
        if (permissions != null) {
            return (SqlBasicCall) parser.parseExpression(permissions);
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
        System.setProperty(Constant.EXECUTE_USERNAME, username);
        return tableEnv.executeSql(singleSql);
    }

    public Table<String, String, String> getRowLevelPermissions() {
        return rowLevelPermissions;
    }

    public void setRowLevelPermissions(Table<String, String, String> rowLevelPermissions) {
        this.rowLevelPermissions = rowLevelPermissions;
    }
}

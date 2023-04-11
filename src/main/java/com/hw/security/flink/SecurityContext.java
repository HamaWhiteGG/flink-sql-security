package com.hw.security.flink;

import com.google.common.collect.Table;
import lombok.SneakyThrows;
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

import java.security.SecureRandom;
import java.util.Optional;

/**
 * @description: SecurityContext
 * @author: HamaWhite
 */
public class SecurityContext {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);

    private TableEnvironmentImpl tableEnv;

    private final ParserImpl parser;

    private final Table<String, String, String> rowLevelPermissions;

    public SecurityContext(Table<String, String, String> rowLevelPermissions) {
        this.rowLevelPermissions = rowLevelPermissions;
        // init table environment
        initTableEnvironment();
        this.parser = (ParserImpl) tableEnv.getParser();
    }

    private void initTableEnvironment() {
        Configuration configuration = new Configuration();

        int port = generatePort();
        configuration.setInteger("rest.port", port);
        LOG.info("WebUI is http://127.0.0.1:{}", port);
        configuration.setBoolean("table.dynamic-table-options.enabled", true);

        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration)) {
            env.enableCheckpointing(3 * 1000L).setParallelism(1);

            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();
            this.tableEnv = (TableEnvironmentImpl) StreamTableEnvironment.create(env, settings);

        } catch (Exception e) {
            throw new FlinkRuntimeException("init local flink execution environment error", e);
        }
    }

    /**
     * Add row-level filter conditions and return new SQL
     */
    public String addRowFilter(String username, String singleSql) {
        // parsing sql and return the abstract syntax tree
        SqlNode sqlNode = parser.parseSql(singleSql);

        // add row-level filtering based on user-configured permission points
        RowFilterVisitor visitor = new RowFilterVisitor(this, username);
        sqlNode.accept(visitor);

        return sqlNode.toString();
    }

    /**
     * Query the configured permission point according to the username and table name, and return
     * it to SqlBasicCall
     */
    public Optional<SqlBasicCall> queryPermissions(String username, String tableName) {
        String permissions = rowLevelPermissions.get(username, tableName);
        LOG.info("username: {}, tableName: {}, permissions: {}", username, tableName, permissions);
        if (permissions != null) {
            // parses a sql expression into a SqlNode.
            return Optional.of((SqlBasicCall) parser.parseExpression(permissions));
        }
        return Optional.empty();
    }

    /**
     * Execute the single sql without user permissions
     */
    public TableResult execute(String singleSql) {
        LOG.info("execute sql: {}", singleSql);
        return tableEnv.executeSql(singleSql);
    }

    /**
     * Execute the single sql with user permissions
     */
    public TableResult execute(String username, String singleSql) {
        LOG.info("origin sql: {}", singleSql);
        String rowFilterSql = addRowFilter(username, singleSql);
        LOG.info("row filter sql: {}", rowFilterSql);

        return tableEnv.executeSql(rowFilterSql);
    }

    @SneakyThrows
    private int generatePort() {
        return SecureRandom.getInstanceStrong().nextInt((8188 - 8082) + 1) + 8082;
    }

    public void addPermission(String username, String tableName, String permission) {
        rowLevelPermissions.put(username, tableName, permission);
    }

    public void deletePermission(String username, String tableName) {
        rowLevelPermissions.remove(username, tableName);
    }
}

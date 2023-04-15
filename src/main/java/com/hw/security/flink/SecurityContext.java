package com.hw.security.flink;

import com.hw.security.flink.model.ColumnEntity;
import com.hw.security.flink.model.TableEntity;
import com.hw.security.flink.visitor.DataMaskVisitor;
import com.hw.security.flink.visitor.RowFilterVisitor;
import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.flink.table.api.Schema.*;

/**
 * @description: SecurityContext
 * @author: HamaWhite
 */
public class SecurityContext {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);

    private TableEnvironmentImpl tableEnv;

    private final ParserImpl parser;

    private final PolicyManager policyManager;


    public SecurityContext(PolicyManager policyManager) {
        this.policyManager = policyManager;
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
    public String applyRowFilter(String username, String singleSql) {
        // parsing sql and return the abstract syntax tree
        SqlNode sqlNode = parser.parseSql(singleSql);

        // add row-level filtering based on user-configured permission points
        RowFilterVisitor visitor = new RowFilterVisitor(this, username);
        sqlNode.accept(visitor);

        return sqlNode.toString();
    }

    /**
     * Add column masking and return new SQL
     */
    public String applyDataMask(String username, String singleSql) {
        // parsing sql and return the abstract syntax tree
        SqlNode sqlNode = parser.parseSql(singleSql);

        // add column masking based on user-configured permission points
        DataMaskVisitor visitor = new DataMaskVisitor(this, username);
        sqlNode.accept(visitor);

        return sqlNode.toString();
    }


    public SqlNode parseExpression(String sqlExpression) {
        return parser.parseExpression(sqlExpression);
    }

    /**
     * Execute the single sql without user permissions
     */
    public TableResult execute(String singleSql) {
        LOG.info("Execute SQL: {}", singleSql);
        return tableEnv.executeSql(singleSql);
    }

    /**
     * Execute the single sql with user permissions
     */
    public TableResult execute(String username, String singleSql) {
        LOG.info("Execute origin SQL: {}", singleSql);
        String rowFilterSql = applyRowFilter(username, singleSql);
        LOG.info("Execute row-filter SQL: {}", rowFilterSql);
        LOG.debug("Explain row-filter SQL: {}", tableEnv.explainSql(rowFilterSql));
        return tableEnv.executeSql(rowFilterSql);
    }

    private Catalog getCatalog(String catalogName) {
        return tableEnv.getCatalog(catalogName).orElseThrow(() ->
                new ValidationException(String.format("Catalog %s does not exist", catalogName))
        );
    }

    public TableEntity getTable(String tableName) {
        return getTable(tableEnv.getCurrentCatalog(), tableEnv.getCurrentDatabase(), tableName);
    }


    public String getCurrentCatalog() {
        return tableEnv.getCurrentCatalog();
    }

    public String getCurrentDatabase() {
        return tableEnv.getCurrentDatabase();
    }

    public PolicyManager getPolicyManager() {
        return policyManager;
    }


    public TableEntity getTable(String database, String tableName) {
        return getTable(tableEnv.getCurrentCatalog(), database, tableName);
    }

    public TableEntity getTable(String catalogName, String database, String tableName) {
        ObjectPath objectPath = new ObjectPath(database, tableName);
        try {
            CatalogBaseTable table = getCatalog(catalogName).getTable(objectPath);
            Schema schema = table.getUnresolvedSchema();
            LOG.info("table.schema: {}", schema);

            List<ColumnEntity> columnList = schema.getColumns()
                    .stream()
                    .map(column -> new ColumnEntity(column.getName(), processColumnType(column)))
                    .collect(Collectors.toList());

            return new TableEntity(tableName, columnList);
        } catch (TableNotExistException e) {
            throw new TableException(String.format(
                    "Cannot find table '%s' in the database %s of catalog %s .", tableName, database, catalogName));
        }
    }

    private String processColumnType(UnresolvedColumn column) {
        if (column instanceof UnresolvedComputedColumn) {
            return ((UnresolvedComputedColumn) column)
                    .getExpression()
                    .asSummaryString();
        } else if (column instanceof UnresolvedPhysicalColumn) {
            return ((UnresolvedPhysicalColumn) column).getDataType()
                    .toString()
                    // delete NOT NULL
                    .replace("NOT NULL", "")
                    .trim();
        } else if (column instanceof UnresolvedMetadataColumn) {
            return ((UnresolvedMetadataColumn) column).getDataType().toString();
        } else {
            throw new IllegalArgumentException("Unsupported column type: " + column);
        }
    }

    @SneakyThrows
    private int generatePort() {
        return SecureRandom.getInstanceStrong().nextInt((8188 - 8082) + 1) + 8082;
    }
}

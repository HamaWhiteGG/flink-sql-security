package com.hw.security.flink.visitor;

import com.google.common.collect.ImmutableList;
import com.hw.security.flink.SecurityContext;
import com.hw.security.flink.model.ColumnEntity;
import com.hw.security.flink.model.DataMaskType;
import com.hw.security.flink.model.TableEntity;
import com.hw.security.flink.util.DataMaskUtils;
import com.hw.security.flink.visitor.basic.AbstractBasicVisitor;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @description: DataMaskVisitor
 * @author: HamaWhite
 */
public class DataMaskVisitor extends AbstractBasicVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(DataMaskVisitor.class);

    public DataMaskVisitor(SecurityContext context, String username) {
        super(context, username);
    }

    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) call;
            // add column masking
            sqlSelect.setFrom(replaceFrom(sqlSelect.getFrom()));
        }
        return super.visit(call);
    }

    private SqlNode replaceFrom(SqlNode from) {
        if (from instanceof SqlIdentifier) {
            String tableName = from.toString();
            LOG.info("测试(SqlIdentifier)--tableName: [{}], tableAlias: [{}]", tableName);
            return addDataMask(from, tableName, tableName);
        } else if (from instanceof SqlJoin) {
            SqlJoin sqlJoin = (SqlJoin) from;
            // support recursive processing, such as join for three tables, process left sqlNode
            sqlJoin.setLeft(replaceFrom(sqlJoin.getLeft()));
            // process right sqlNode
            sqlJoin.setRight(replaceFrom(sqlJoin.getRight()));
            return from;
        } else if (from instanceof SqlBasicCall) {
            // Table has an alias or comes from a sub-query
            SqlNode[] tableNodes = ((SqlBasicCall) from).getOperands();
            /*
              If there is a sub-query in the Join, row-level filtering has been appended to the sub-query.
              What is returned here is the SqlSelect type, just return the original where directly
             */
            if (!(tableNodes[0] instanceof SqlIdentifier)) {
                return from;
            }
            String tableName = tableNodes[0].toString();
            String tableAlias = tableNodes[1].toString();
            LOG.info("测试(SqlBasicCall)--tableName: [{}], tableAlias: [{}]", tableName, tableAlias);
            return addDataMask(from, tableName, tableAlias);
        }
        return from;
    }

    private SqlNode addDataMask(SqlNode from, String tableName, String tableAlias) {
        TableEntity table = securityContext.getTable(tableName);
        boolean doColumnMasking = false;
        List<String> columnTransformerList = new ArrayList<>();
        for (ColumnEntity column : table.getColumnList()) {
            String columnTransformer = column.getColumnName();
            Optional<String> condition = policyManager.getDataMaskCondition(username
                    , securityContext.getCurrentCatalog()
                    , securityContext.getCurrentDatabase()
                    , tableName
                    , column.getColumnName());
            if (condition.isPresent()) {
                doColumnMasking = true;
                DataMaskType maskType = DataMaskUtils.getDataMaskType(condition.get());
                columnTransformer = maskType.getTransformer().replace("{col}", column.getColumnName());
            }
            columnTransformerList.add(columnTransformer);
        }
        if (doColumnMasking) {
            String replaceText = buildReplaceText(table, columnTransformerList);
            SqlSelect sqlSelect = (SqlSelect) securityContext.parseExpression(replaceText);
            SqlNode[] operands = new SqlNode[2];
            operands[0] = sqlSelect;
            operands[1] = new SqlIdentifier(ImmutableList.of(tableAlias)
                    , null
                    , new SqlParserPos(0, 0)
                    , null
            );
            return new SqlBasicCall(new SqlAsOperator()
                    , operands
                    , new SqlParserPos(0, 0)
            );
        }
        return from;
    }


    private String buildReplaceText(TableEntity table, List<String> columnTransformerList) {
        StringBuilder sb = new StringBuilder();
        sb.append("(SELECT ");
        boolean firstOne = true;
        for (int index = 0; index < columnTransformerList.size(); index++) {
            String transformer = columnTransformerList.get(index);
            if (!firstOne) {
                sb.append(", ");
            } else {
                firstOne = false;
            }
            ColumnEntity column = table.getColumnList().get(index);
            String colName = column.getColumnName();
            if (!transformer.equals(colName)) {
                // CAST(transformer AS COL_TYPE) AS COL_NAME
                sb.append("CAST(" + transformer + " AS " + column.getColumnType() + ") AS " + column.getColumnName());
            } else {
                sb.append(column.getColumnName());
            }
        }
        sb.append(" FROM ");
        sb.append(table.getTableName());
        sb.append(")");
        return sb.toString();
    }
}




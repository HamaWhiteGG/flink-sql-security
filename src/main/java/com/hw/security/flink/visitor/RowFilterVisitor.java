/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hw.security.flink.visitor;

import com.google.common.collect.ImmutableList;
import com.hw.security.flink.SecurityContext;
import com.hw.security.flink.visitor.basic.AbstractBasicVisitor;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * @description: RowFilterVisitor
 * @author: HamaWhite
 */
public class RowFilterVisitor extends AbstractBasicVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(RowFilterVisitor.class);

    public RowFilterVisitor(SecurityContext context, String username) {
        super(context, username);
    }

    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) call;

            SqlNode originWhere = sqlSelect.getWhere();
            // add row level filter condition for where clause
            SqlNode rowFilterWhere = addCondition(sqlSelect.getFrom(), originWhere);
            if (rowFilterWhere != originWhere) {
                LOG.info("Rewritten SQL based on row-level privilege filtering for user [{}]", username);
            }
            sqlSelect.setWhere(rowFilterWhere);
        }
        return super.visit(call);
    }

    /**
     * The main process of controlling row-level permissions
     */
    private SqlNode addCondition(SqlNode from, SqlNode where) {
        if (from instanceof SqlJoin) {
            SqlJoin sqlJoin = (SqlJoin) from;
            // support recursive processing, such as join for three tables, process left sqlNode
            where = addCondition(sqlJoin.getLeft(), where);
            // process right sqlNode
            return addCondition(sqlJoin.getRight(), where);
        } else if (from instanceof SqlBasicCall) {
            SqlNode[] tableNodes = ((SqlBasicCall) from).getOperands();
            /*
             * If there is a sub-query in the Join, row-level filtering has been appended to the sub-query. What is
             * returned here is the SqlSelect type, just return the original where directly
             */
            if (!(tableNodes[0] instanceof SqlIdentifier)) {
                return where;
            }
            String tablePath = tableNodes[0].toString();
            String tableAlias = tableNodes[1].toString();
            LOG.debug("SqlBasicCall-tablePath: [{}], tableAlias: [{}]", tablePath, tableAlias);
            return addRowFilter(where, tablePath, tableAlias);
        }
        return where;
    }

    /**
     * Add row-level filtering based on user-configured permission points
     */
    private SqlNode addRowFilter(SqlNode where, String tablePath, String tableAlias) {
        ObjectIdentifier tableIdentifier = toObjectIdentifier(tablePath);

        Optional<String> condition = policyManager.getRowFilterCondition(username,
                tableIdentifier.getCatalogName(),
                tableIdentifier.getDatabaseName(),
                tableIdentifier.getObjectName());

        if (condition.isPresent()) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) securityContext.parseExpression(condition.get());
            ImmutableList<String> namesList = ImmutableList.of(tableAlias, sqlBasicCall.getOperands()[0].toString());
            sqlBasicCall.getOperands()[0] = new SqlIdentifier(namesList, null, new SqlParserPos(0, 0), null);
            return buildWhereClause(where, sqlBasicCall);
        }
        return buildWhereClause(where, null);
    }

    /**
     * Rebuild the where clause
     */
    private SqlNode buildWhereClause(SqlNode where, SqlBasicCall permissions) {
        if (permissions != null) {
            if (where == null) {
                return permissions;
            }
            SqlBinaryOperator sqlBinaryOperator =
                    new SqlBinaryOperator(SqlKind.AND.name(), SqlKind.AND, 0, true, null, null, null);
            SqlNode[] operands = new SqlNode[2];
            operands[0] = where;
            operands[1] = permissions;
            SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
            return new SqlBasicCall(sqlBinaryOperator, operands, sqlParserPos);
        }
        return where;
    }
}

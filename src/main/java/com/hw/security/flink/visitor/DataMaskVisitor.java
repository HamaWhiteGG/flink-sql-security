package com.hw.security.flink.visitor;

import com.hw.security.flink.SecurityContext;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description: DataMaskVisitor
 * @author: HamaWhite
 */
public class DataMaskVisitor extends SqlBasicVisitor<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(DataMaskVisitor.class);

    private final SecurityContext context;

    private final String username;

    public DataMaskVisitor(SecurityContext context, String username) {
        this.context = context;
        this.username = username;
    }


    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) call;
            // add column masking
            replaceFrom(sqlSelect.getFrom(), false);
        }
        return super.visit(call);
    }

    /**
     * The main process of controlling row-level permissions
     */
    private void replaceFrom(SqlNode from, boolean fromJoin) {
        if (from instanceof SqlIdentifier) {
            String tableName = from.toString();
            // the table name is used as an alias for join
            String tableAlias = fromJoin ? tableName : null;
            LOG.info("测试(SqlIdentifier)--tableName: [{}], tableAlias: [{}]",tableName,tableAlias);
        } else if (from instanceof SqlJoin) {
            SqlJoin sqlJoin = (SqlJoin) from;
            // support recursive processing, such as join for three tables, process left sqlNode
            replaceFrom(sqlJoin.getLeft(), true);
            // process right sqlNode
            replaceFrom(sqlJoin.getRight(), true);
        } else if (from instanceof SqlBasicCall) {
            // Table has an alias or comes from a sub-query
            SqlNode[] tableNodes = ((SqlBasicCall) from).getOperands();
            /*
              If there is a sub-query in the Join, row-level filtering has been appended to the sub-query.
              What is returned here is the SqlSelect type, just return the original where directly
             */
            if (!(tableNodes[0] instanceof SqlIdentifier)) {
                return ;
            }
            String tableName = tableNodes[0].toString();
            String tableAlias = tableNodes[1].toString();
            LOG.info("测试(SqlBasicCall)--tableName: [{}], tableAlias: [{}]",tableName,tableAlias);
        }
    }
}




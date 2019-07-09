package com.flipkart.foxtrot.sql;

import com.flipkart.foxtrot.sql.query.FqlActionQuery;
import com.flipkart.foxtrot.sql.query.FqlDescribeTable;
import com.flipkart.foxtrot.sql.query.FqlShowTablesQuery;

public interface FqlQueryVisitor {
    void visit(FqlDescribeTable fqlDescribeTable) throws Exception;

    void visit(FqlShowTablesQuery fqlShowTablesQuery) throws Exception;

    void visit(FqlActionQuery fqlActionQuery) throws Exception;
}

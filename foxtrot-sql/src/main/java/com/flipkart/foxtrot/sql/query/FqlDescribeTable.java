package com.flipkart.foxtrot.sql.query;

import com.flipkart.foxtrot.sql.FqlQuery;
import com.flipkart.foxtrot.sql.FqlQueryVisitor;

public class FqlDescribeTable implements FqlQuery {

    private final String tableName;

    public FqlDescribeTable(final String tableName) {
        super();
        this.tableName = tableName;
    }

    @Override
    public void receive(FqlQueryVisitor visitor) {
        visitor.visit(this);
    }

    public String getTableName() {
        return tableName;
    }
}

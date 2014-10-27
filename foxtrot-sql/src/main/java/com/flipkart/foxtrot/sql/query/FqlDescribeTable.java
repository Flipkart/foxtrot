package com.flipkart.foxtrot.sql.query;

import com.flipkart.foxtrot.sql.FqlQuery;
import com.flipkart.foxtrot.sql.FqlQueryType;
import com.flipkart.foxtrot.sql.FqlQueryVisitor;

public class FqlDescribeTable extends FqlQuery {
    private final String tableName;

    public FqlDescribeTable(final String tableName) {
        super(FqlQueryType.desc);
        this.tableName = tableName;
    }

    @Override
    public void receive(FqlQueryVisitor visitor) throws Exception {
        visitor.visit(this);
    }

    public String getTableName() {
        return tableName;
    }
}

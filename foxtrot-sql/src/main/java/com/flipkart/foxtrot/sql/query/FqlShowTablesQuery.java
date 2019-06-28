package com.flipkart.foxtrot.sql.query;

import com.flipkart.foxtrot.sql.FqlQuery;
import com.flipkart.foxtrot.sql.FqlQueryVisitor;

public class FqlShowTablesQuery implements FqlQuery {

    public FqlShowTablesQuery() {
        super();
    }

    @Override
    public void receive(FqlQueryVisitor visitor) {
        visitor.visit(this);
    }

}

package com.flipkart.foxtrot.sql.query;

import com.flipkart.foxtrot.sql.FqlQuery;
import com.flipkart.foxtrot.sql.FqlQueryType;
import com.flipkart.foxtrot.sql.FqlQueryVisitor;

public class FqlShowTablesQuery extends FqlQuery {

    public FqlShowTablesQuery() {
        super(FqlQueryType.showtables);
    }

    @Override
    public void receive(FqlQueryVisitor visitor) throws Exception {
        visitor.visit(this);
    }

}

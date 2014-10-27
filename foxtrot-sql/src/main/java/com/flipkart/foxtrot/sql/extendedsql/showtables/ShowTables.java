package com.flipkart.foxtrot.sql.extendedsql.showtables;

import com.flipkart.foxtrot.sql.extendedsql.ExtendedSqlStatement;
import com.flipkart.foxtrot.sql.extendedsql.ExtendedSqlStatementVisitor;

public class ShowTables implements ExtendedSqlStatement {
    @Override
    public void receive(ExtendedSqlStatementVisitor visitor) {
        visitor.visit(this);
    }
}

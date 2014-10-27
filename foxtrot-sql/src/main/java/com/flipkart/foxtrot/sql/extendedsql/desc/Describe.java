package com.flipkart.foxtrot.sql.extendedsql.desc;

import com.flipkart.foxtrot.sql.extendedsql.ExtendedSqlStatement;
import com.flipkart.foxtrot.sql.extendedsql.ExtendedSqlStatementVisitor;
import net.sf.jsqlparser.schema.Table;

public class Describe implements ExtendedSqlStatement {
    private final Table table;

    public Describe(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    @Override
    public void receive(ExtendedSqlStatementVisitor visitor) {
        visitor.visit(this);
    }
}

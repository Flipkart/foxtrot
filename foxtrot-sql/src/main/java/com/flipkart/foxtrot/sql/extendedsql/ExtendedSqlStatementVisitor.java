package com.flipkart.foxtrot.sql.extendedsql;

import com.flipkart.foxtrot.sql.extendedsql.desc.Describe;
import com.flipkart.foxtrot.sql.extendedsql.showtables.ShowTables;

public interface ExtendedSqlStatementVisitor {
    void visit(Describe describe);

    void visit(ShowTables showTables);
}

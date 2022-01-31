package com.flipkart.foxtrot.sql.extendedsql;

public interface ExtendedSqlStatement {
    void receive(ExtendedSqlStatementVisitor visitor);
}

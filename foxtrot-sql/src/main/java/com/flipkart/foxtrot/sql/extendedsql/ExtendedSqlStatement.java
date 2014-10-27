package com.flipkart.foxtrot.sql.extendedsql;

public interface ExtendedSqlStatement {
    public void receive(ExtendedSqlStatementVisitor visitor);
}

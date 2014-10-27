package com.flipkart.foxtrot.sql;

public abstract class FqlQuery {
    private final FqlQueryType type;

    protected FqlQuery(FqlQueryType type) {
        this.type = type;
    }

    abstract public void receive(FqlQueryVisitor visitor) throws Exception;
}

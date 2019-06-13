package com.flipkart.foxtrot.sql;

public interface FqlQuery {

    void receive(FqlQueryVisitor visitor);
}

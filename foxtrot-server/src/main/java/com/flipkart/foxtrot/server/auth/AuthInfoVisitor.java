package com.flipkart.foxtrot.server.auth;

/**
 *
 */
public interface AuthInfoVisitor<T> {
    T visit(TokenAuthInfo tokenAuthInfo);
}

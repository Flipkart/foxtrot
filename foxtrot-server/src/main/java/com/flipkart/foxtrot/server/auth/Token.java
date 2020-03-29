package com.flipkart.foxtrot.server.auth;

import lombok.Value;

import java.util.Date;

/**
 *
 */
@Value
public class Token {
    public static final Token DEFAULT = new Token("__DEFAULT_TOKEN__", TokenType.SYSTEM, "__DEFAULT__", null);

    String id;
    TokenType tokenType;
    String userId;
    Date expiry;
}

package com.flipkart.foxtrot.server.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
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

    public Token(
            @JsonProperty("id") String id,
            @JsonProperty("tokenType") TokenType tokenType,
            @JsonProperty("userId") String userId,
            @JsonProperty("expiry") Date expiry) {
        this.id = id;
        this.tokenType = tokenType;
        this.userId = userId;
        this.expiry = expiry;
    }
}

package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.server.auth.authprovider.IdType;
import lombok.Value;

import java.util.Date;

/**
 *
 */
@Value
public class Token {
    public static final Token DEFAULT = new Token("__DEFAULT_TOKEN__",
                                                  IdType.SESSION_ID,
                                                  TokenType.SYSTEM,
                                                  "__DEFAULT__",
                                                  null);

    String id;
    IdType idType;
    TokenType tokenType;
    String userId;
    Date expiry;
}

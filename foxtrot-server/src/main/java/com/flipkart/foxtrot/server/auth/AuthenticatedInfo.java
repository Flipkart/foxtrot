package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.core.auth.User;
import lombok.Value;

/**
 *
 */
@Value
public class AuthenticatedInfo {
    Token token;
    User user;
}

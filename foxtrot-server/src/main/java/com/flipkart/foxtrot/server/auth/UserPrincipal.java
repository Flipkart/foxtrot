package com.flipkart.foxtrot.server.auth;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.foxtrot.core.auth.User;
import java.security.Principal;
import lombok.Value;

/**
 *
 */
@Value
public class UserPrincipal implements Principal {

    public static final UserPrincipal DEFAULT = new UserPrincipal(User.DEFAULT, Token.DEFAULT);

    User user;
    Token token;

    @Override
    @JsonIgnore
    public String getName() {
        return user.getId();
    }
}

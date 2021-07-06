package com.flipkart.foxtrot.server.auth.authprovider;

import com.flipkart.foxtrot.server.auth.AuthInfo;
import com.flipkart.foxtrot.server.auth.AuthenticatedInfo;
import com.flipkart.foxtrot.server.auth.Token;

import java.util.Optional;

/**
 *
 */
public interface AuthProvider {

    AuthType type();

    String redirectionURL(String sessionId);

    default boolean isPreregistrationRequired() {
        return true;
    }

    Optional<Token> login(String authCode, String sessionId);

    Optional<AuthenticatedInfo> authenticate(AuthInfo authInfo);
}

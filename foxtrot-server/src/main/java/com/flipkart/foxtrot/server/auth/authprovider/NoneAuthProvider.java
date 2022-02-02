package com.flipkart.foxtrot.server.auth.authprovider;

import com.flipkart.foxtrot.server.auth.AuthInfo;
import com.flipkart.foxtrot.server.auth.AuthenticatedInfo;
import com.flipkart.foxtrot.server.auth.Token;

import java.util.Optional;

/**
 *
 */
public class NoneAuthProvider implements AuthProvider {
    @Override
    public AuthType type() {
        return AuthType.NONE;
    }

    @Override
    public String redirectionURL(String sessionId) {
        throw new IllegalStateException("Redirection called on NONE auth mode");
    }

    @Override
    public Optional<Token> login(String authCode, String sessionId) {
        return Optional.empty();
    }

    @Override
    public Optional<AuthenticatedInfo> authenticate(AuthInfo authInfo) {
        return Optional.empty();
    }

    @Override
    public boolean logout(String sessionId) {
        return true;
    }
}

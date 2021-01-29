package com.flipkart.foxtrot.server.auth.authprovider;

import com.flipkart.foxtrot.server.auth.Token;
import lombok.Builder;
import lombok.Data;

import java.util.Optional;

/**
 *
 */
public interface AuthProvider {


    @Data
    @Builder
    public class AuthInfo {
        private final String ip;
        private final String email;
    }

    AuthType type();

    String redirectionURL(String sessionId);

    default boolean isPreregistrationRequired() {
        return true;
    }

    Optional<Token> login(String authCode, String sessionId);
}

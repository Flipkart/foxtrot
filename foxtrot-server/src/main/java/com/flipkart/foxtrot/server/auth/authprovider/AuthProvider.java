package com.flipkart.foxtrot.server.auth.authprovider;

import com.flipkart.foxtrot.server.auth.Token;
import java.util.Optional;
import lombok.Builder;
import lombok.Data;

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

    @Data
    @Builder
    public class AuthInfo {

        private final String ip;
        private final String email;
    }
}

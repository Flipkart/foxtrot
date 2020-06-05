package com.flipkart.foxtrot.server.auth;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.JwtContext;

/**
 * Authenticator that will be run
 */
@Slf4j
@Singleton
public class TokenAuthenticator implements Authenticator<JwtContext, UserPrincipal> {

    private final AuthConfig config;
    private final Provider<AuthStore> authStore;

    @Inject
    public TokenAuthenticator(AuthConfig config,
                              final Provider<AuthStore> authStore) {
        this.config = config;
        this.authStore = authStore;
    }

    @Override
    public Optional<UserPrincipal> authenticate(JwtContext jwtContext) throws AuthenticationException {
        if (!config.isEnabled()) {
            log.debug("Authentication is disabled");
            return Optional.of(UserPrincipal.DEFAULT);
        }
        log.debug("Auth called");
        final String userId;
        final String tokenId;
        try {
            val claims = jwtContext.getJwtClaims();
            userId = claims.getSubject();
            tokenId = claims.getJwtId();
        } catch (MalformedClaimException e) {
            log.error(String.format("exception in claim extraction %s", e.getMessage()), e);
            return Optional.empty();
        }
        log.debug("authentication_requested userId:{} tokenId:{}", userId, tokenId);
        val token = authStore.get()
                .getToken(tokenId)
                .orElse(null);
        if (token == null) {
            log.warn("authentication_failed::invalid_session userId:{} tokenId:{}", userId, tokenId);
            return Optional.empty();
        }
        if (!token.getUserId()
                .equals(userId)) {
            log.warn("authentication_failed::user_mismatch userId:{} tokenId:{}", userId, tokenId);
            return Optional.empty();
        }
        val user = authStore.get()
                .getUser(token.getUserId())
                .orElse(null);
        if (null == user) {
            log.warn("authentication_failed::invalid_user userId:{} tokenId:{}", userId, tokenId);
            return Optional.empty();
        }
        log.debug("authentication_success userId:{} tokenId:{}", userId, tokenId);
        return Optional.of(new UserPrincipal(user, token));
    }
}

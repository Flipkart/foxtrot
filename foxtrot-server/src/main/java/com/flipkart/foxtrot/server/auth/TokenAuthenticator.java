package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.server.auth.authprovider.AuthProvider;
import com.google.common.base.Strings;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Optional;

/**
 * Authenticator that will be run
 */
@Slf4j
@Singleton
public class TokenAuthenticator implements Authenticator<String, UserPrincipal> {

    private final AuthConfig config;
    private final Provider<AuthProvider> provider;

    @Inject
    public TokenAuthenticator(AuthConfig config, Provider<AuthProvider> provider) {
        this.config = config;
        this.provider = provider;
    }

    @Override
    public Optional<UserPrincipal> authenticate(String token) throws AuthenticationException {
        if(!config.isEnabled()) {
            log.debug("Authentication is disabled");
            return Optional.of(UserPrincipal.DEFAULT);
        }
        log.debug("Auth called");
        if(Strings.isNullOrEmpty(token)) {
            log.warn("authentication_failed::empty token");
            return Optional.empty();
        }
        val info = provider.get()
                .authenticate(new TokenAuthInfo(token))
                .orElse(null);
        if (info == null) {
            log.warn("authentication_failed::token_validation_failed");
            return Optional.empty();
        }
        return Optional.of(new UserPrincipal(info.getUser(), info.getToken()));
    }
}

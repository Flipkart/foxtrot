package com.flipkart.foxtrot.server.auth.authprovider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.AuthStore;
import com.flipkart.foxtrot.server.auth.authprovider.impl.*;

/**
 *
 */
public class ConfiguredAuthProviderFactory implements AuthProviderFactory {
    private final AuthConfig authConfig;

    public ConfiguredAuthProviderFactory(AuthConfig authConfig) {
        this.authConfig = authConfig;
    }

    @Override
    public AuthProvider build(ObjectMapper mapper, AuthStore authStore) {
        return authConfig.getProvider()
                .accept(new AuthConfigVisitor<AuthProvider>() {
                    @Override
                    public AuthProvider visit(GoogleAuthProviderConfig googleAuthConfig) {
                        return new GoogleAuthProvider(googleAuthConfig, authConfig, mapper, authStore);
                    }

                    @Override
                    public AuthProvider visit(IdmanAuthProviderConfig idmanAuthProviderConfig) {
                        return new IdmanAuthProvider(idmanAuthProviderConfig, mapper);
                    }

                    @Override
                    public AuthProvider visit(NoneAuthProviderConfig noneAuthProviderConfig) {
                        return new NoneAuthProvider();
                    }
                });
    }
}

package com.flipkart.foxtrot.server.auth.authprovider;

import java.util.Map;

/**
 *
 */
public interface AuthProviderFactory {
    Map<AuthType, AuthProvider> build();
}

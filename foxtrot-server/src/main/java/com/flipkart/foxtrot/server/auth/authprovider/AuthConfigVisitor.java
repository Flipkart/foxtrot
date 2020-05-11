package com.flipkart.foxtrot.server.auth.authprovider;

import com.flipkart.foxtrot.server.auth.authprovider.impl.GoogleAuthProviderConfig;

/**
 *
 */
public interface AuthConfigVisitor<T> {
    T visit(GoogleAuthProviderConfig googleAuthConfig);
}

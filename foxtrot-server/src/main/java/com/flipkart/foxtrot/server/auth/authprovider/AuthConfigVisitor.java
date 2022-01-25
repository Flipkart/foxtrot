package com.flipkart.foxtrot.server.auth.authprovider;

import com.flipkart.foxtrot.server.auth.authprovider.impl.GoogleAuthProviderConfig;
import com.flipkart.foxtrot.server.auth.authprovider.impl.IdmanAuthProviderConfig;
import com.flipkart.foxtrot.server.auth.authprovider.impl.NoneAuthProviderConfig;

/**
 *
 */
public interface AuthConfigVisitor<T> {
    T visit(GoogleAuthProviderConfig googleAuthConfig);

    T visit(IdmanAuthProviderConfig idmanAuthProviderConfig);

    T visit(NoneAuthProviderConfig noneAuthProviderConfig);
}

package com.flipkart.foxtrot.server.auth.authprovider.impl;

import com.flipkart.foxtrot.server.auth.authprovider.AuthConfigVisitor;
import com.flipkart.foxtrot.server.auth.authprovider.AuthProviderConfig;
import com.flipkart.foxtrot.server.auth.authprovider.AuthType;

/**
 *
 */
public class NoneAuthProviderConfig extends AuthProviderConfig {

    public NoneAuthProviderConfig() {
        super(AuthType.NONE);
    }

    @Override
    public <T> T accept(AuthConfigVisitor<T> visitor) {
        return visitor.visit(this);
    }
}

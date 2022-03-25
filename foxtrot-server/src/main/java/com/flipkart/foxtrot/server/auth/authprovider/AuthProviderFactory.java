package com.flipkart.foxtrot.server.auth.authprovider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.server.auth.AuthStore;

/**
 *
 */
public interface AuthProviderFactory {

    AuthProvider build(ObjectMapper mapper, AuthStore authStore);
}

package com.flipkart.foxtrot.server.auth.authprovider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.server.auth.AuthStore;
import com.google.inject.Injector;

/**
 *
 */
public interface AuthProviderFactory {

    AuthProvider build(ObjectMapper mapper, AuthStore authStore);
}

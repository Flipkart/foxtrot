package com.flipkart.foxtrot.server.auth;

import io.dropwizard.auth.*;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtContext;

import javax.inject.Inject;
import javax.ws.rs.ext.Provider;

/**
 *
 */
@Provider
public class JwtAuthDynamicFeature extends AuthDynamicFeature {
    @Inject
    public JwtAuthDynamicFeature(
            AuthConfig authConfig,
            JwtConsumer jwtConsumer,
            Authorizer<UserPrincipal> authorizer,
            Authenticator<JwtContext, UserPrincipal> authenticator,
            Environment environment) {
        super(new JwtAuthFilter.Builder()
                .setAuthConfig(authConfig)
                .setJwtConsumer(jwtConsumer)
                .setCookieName("token")
                .setPrefix("Bearer")
                .setRealm("realm")
                .setAuthenticator(authenticator)
                .setAuthorizer(authorizer)
                .setUnauthorizedHandler(new DefaultUnauthorizedHandler())
                .buildAuthFilter());
        if(null != environment) {
            environment.jersey().register(new AuthValueFactoryProvider.Binder<>(UserPrincipal.class));
            environment.jersey().register(RolesAllowedDynamicFeature.class);
        }
    }
}

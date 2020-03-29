package com.flipkart.foxtrot.server.auth;

import com.github.toastshaman.dropwizard.auth.jwt.JwtAuthFilter;
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
            JwtConsumer jwtConsumer,
            Authorizer<UserPrincipal> authorizer,
            Authenticator<JwtContext, UserPrincipal> authenticator,
            Environment environment) {
        super(new JwtAuthFilter.Builder<UserPrincipal>()
                .setJwtConsumer(jwtConsumer)
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

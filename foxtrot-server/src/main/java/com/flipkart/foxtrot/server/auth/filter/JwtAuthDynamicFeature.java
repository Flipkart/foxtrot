package com.flipkart.foxtrot.server.auth.filter;

import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.UserPrincipal;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.Authorizer;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;

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
            Authorizer<UserPrincipal> authorizer,
            Environment environment) {
        super(new UserAuthorizationFilter(authConfig, authorizer));
        if (null != environment) {
            environment.jersey().register(new AuthValueFactoryProvider.Binder<>(UserPrincipal.class));
            environment.jersey().register(RolesAllowedDynamicFeature.class);
        }
    }
}

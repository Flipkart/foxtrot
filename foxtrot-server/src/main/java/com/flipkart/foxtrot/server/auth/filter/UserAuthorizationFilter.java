package com.flipkart.foxtrot.server.auth.filter;

import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.JWTAuthenticationFailure;
import com.flipkart.foxtrot.server.auth.UserPrincipal;
import io.dropwizard.auth.Authorizer;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;

/**
 * This filter assigns role to validated user
 */
@Priority(Priorities.AUTHENTICATION)
public class UserAuthorizationFilter implements ContainerRequestFilter {

    private final AuthConfig authConfig;
    private final Authorizer<UserPrincipal> authorizer;

    public UserAuthorizationFilter(
            AuthConfig authConfig,
            Authorizer<UserPrincipal> authorizer) {
        this.authConfig = authConfig;
        this.authorizer = authorizer;
    }

    @Override
    public void filter(final ContainerRequestContext requestContext) throws IOException {
        if(!authConfig.isEnabled()) {
            updateContext(requestContext, UserPrincipal.DEFAULT);
            return;
        }
        UserPrincipal principal = SessionUser.take();
        if(null != principal) {
            updateContext(requestContext, principal);
            return;
        }
        throw new JWTAuthenticationFailure();
    }

    private void updateContext(ContainerRequestContext requestContext, UserPrincipal principal) {
        requestContext.setSecurityContext(new SecurityContext() {

            @Override
            public Principal getUserPrincipal() {
                return principal;
            }

            @Override
            public boolean isUserInRole(String role) {
                return authorizer.authorize(principal, role);
            }

            @Override
            public boolean isSecure() {
                return requestContext.getSecurityContext().isSecure();
            }

            @Override
            public String getAuthenticationScheme() {
                return SecurityContext.BASIC_AUTH;
            }

        });
    }
}

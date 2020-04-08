package com.flipkart.foxtrot.server.auth;

import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import lombok.val;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

@Priority(Priorities.AUTHENTICATION)
public class JwtAuthFilter extends AuthFilter<JwtContext, UserPrincipal> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JwtAuthFilter.class);

    private final AuthConfig authConfig;
    private final JwtConsumer consumer;
    private final String cookieName;

    private JwtAuthFilter(
            AuthConfig authConfig,
            JwtConsumer consumer,
            String cookieName) {
        this.authConfig = authConfig;
        this.consumer = consumer;
        this.cookieName = cookieName;
    }

    @Override
    public void filter(final ContainerRequestContext requestContext) throws IOException {
        LOGGER.info("AUTH Called");
        if(!authConfig.isEnabled()) {
            updateContext(requestContext, UserPrincipal.DEFAULT);
            return;
        }
        final Optional<String> optionalToken = getTokenFromCookieOrHeader(requestContext);

        if (optionalToken.isPresent()) {
            try {
                val jwtContext = verifyToken(optionalToken.get());
                val principal = authenticator.authenticate(jwtContext);

                if (principal.isPresent()) {
                    updateContext(requestContext, principal.get());
                    return;
                }
            } catch (InvalidJwtException ex) {
                LOGGER.warn("Error decoding credentials: " + ex.getMessage(), ex);
            } catch (AuthenticationException ex) {
                LOGGER.warn("Error authenticating credentials", ex);
                throw new InternalServerErrorException();
            }
        }

        throw new JWTAuthenticationFailure();
//        throw new WebApplicationException(unauthorizedHandler.buildResponse(prefix, realm));
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

    private JwtContext verifyToken(String rawToken) throws InvalidJwtException {
        return consumer.process(rawToken);
    }

    private Optional<String> getTokenFromCookieOrHeader(ContainerRequestContext requestContext) {
        final Optional<String> headerToken = getTokenFromHeader(requestContext.getHeaders());
        return headerToken.isPresent() ? headerToken : getTokenFromCookie(requestContext);
    }

    private Optional<String> getTokenFromHeader(MultivaluedMap<String, String> headers) {
        final String header = headers.getFirst(AUTHORIZATION);
        if (header != null) {
            int space = header.indexOf(' ');
            if (space > 0) {
                final String method = header.substring(0, space);
                if (prefix.equalsIgnoreCase(method)) {
                    final String rawToken = header.substring(space + 1);
                    return Optional.of(rawToken);
                }
            }
        }

        return Optional.empty();
    }

    private Optional<String> getTokenFromCookie(ContainerRequestContext requestContext) {
        final Map<String, Cookie> cookies = requestContext.getCookies();

        if (cookieName != null && cookies.containsKey(cookieName)) {
            final Cookie tokenCookie = cookies.get(cookieName);
            final String rawToken = tokenCookie.getValue();
            return Optional.of(rawToken);
        }

        return Optional.empty();
    }

    /**
     * Builder for {@link JwtAuthFilter}.
     * <p>An {@link Authenticator} must be provided during the building process.</p>
     *
     */
    public static class Builder extends AuthFilterBuilder<JwtContext, UserPrincipal, JwtAuthFilter> {

        private AuthConfig authConfig;
        private JwtConsumer consumer;
        private String cookieName;

        public Builder setAuthConfig(AuthConfig authConfig) {
            this.authConfig = authConfig;
            return this;
        }

        public Builder setJwtConsumer(JwtConsumer consumer) {
            this.consumer = consumer;
            return this;
        }

        public Builder setCookieName(String cookieName) {
            this.cookieName = cookieName;
            return this;
        }

        @Override
        protected JwtAuthFilter newInstance() {
            checkNotNull(consumer, "JwtConsumer is not set");
            return new JwtAuthFilter(authConfig, consumer, cookieName);
        }
    }
}

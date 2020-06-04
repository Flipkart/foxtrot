package com.flipkart.foxtrot.server.auth.filter;

import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.UserPrincipal;
import com.flipkart.foxtrot.server.auth.sessionstore.SessionDataStore;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Priority;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Priorities;
import javax.ws.rs.core.HttpHeaders;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtContext;

/**
 * This filter validates the token
 */
@Priority(Priorities.AUTHENTICATION)
@WebFilter("/*")
@Slf4j
public class UserAuthenticationFilter implements Filter {

    private static final Set<String> WHITELISTED_PATTERNS = ImmutableSet.<String>builder().add("/foxtrot/google")
            .add("^/foxtrot/auth.*")
            .build();
    private final AuthConfig authConfig;
    private final Provider<SessionDataStore> sessionDataStore;
    private final JwtConsumer consumer;
    private final Provider<Authenticator<JwtContext, UserPrincipal>> authenticator;

    @Inject
    public UserAuthenticationFilter(AuthConfig authConfig,
                                    Provider<SessionDataStore> sessionDataStore,
                                    JwtConsumer consumer,
                                    Provider<Authenticator<JwtContext, UserPrincipal>> authenticator) {
        this.authConfig = authConfig;
        this.sessionDataStore = sessionDataStore;
        this.consumer = consumer;
        this.authenticator = authenticator;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain) throws IOException, ServletException {
        if (!authConfig.isEnabled()) {
            log.trace("Auth disabled");
            chain.doFilter(request, response);
            return;
        }
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        final String requestURI = httpRequest.getRequestURI();
        if (WHITELISTED_PATTERNS.stream()
                .anyMatch(requestURI::startsWith)) {
            chain.doFilter(request, response);
            return;
        }
        val jwt = getTokenFromCookieOrHeader(httpRequest).orElse(null);
        if (null != jwt) {
            try {
                final JwtContext context = consumer.process(jwt);
                val principal = authenticator.get()
                        .authenticate(context)
                        .orElse(null);
                if (null != principal) {
                    SessionUser.put(principal);
                    chain.doFilter(request, response);
                    return;
                }
            } catch (InvalidJwtException | AuthenticationException e) {
                log.error("Jwt validation failure: ", e);
            }
        }
        val referrer = httpRequest.getHeader(org.apache.http.HttpHeaders.REFERER);
        val source = Strings.isNullOrEmpty(referrer)
                     ? requestURI
                     : referrer;
        httpResponse.addCookie(new Cookie("redirection", source));
        httpResponse.sendRedirect("/foxtrot/google/login");
    }

    @Override
    public void destroy() {

    }

    private Optional<String> getTokenFromCookieOrHeader(HttpServletRequest servletRequest) {
        val tokenFromHeader = getTokenFromHeader(servletRequest);
        return tokenFromHeader.isPresent()
               ? tokenFromHeader
               : getTokenFromCookie(servletRequest);
    }

    private Optional<String> getTokenFromHeader(HttpServletRequest servletRequest) {
        val header = servletRequest.getHeader(HttpHeaders.AUTHORIZATION);
        if (header != null) {
            int space = header.indexOf(' ');
            if (space > 0) {
                final String method = header.substring(0, space);
                if ("Bearer".equalsIgnoreCase(method)) {
                    final String rawToken = header.substring(space + 1);
                    return Optional.of(rawToken);
                }
            }
        }
        return Optional.empty();
    }

    private Optional<String> getTokenFromCookie(HttpServletRequest request) {
        val cookies = request.getCookies();
        if (null != cookies && cookies.length != 0) {
            val token = Arrays.stream(cookies)
                    .filter(cookie -> cookie.getName()
                            .equals("token"))
                    .findAny()
                    .orElse(null);
            if (null != token) {
                return Optional.of(token.getValue());
            }
        }
        return Optional.empty();
    }

}

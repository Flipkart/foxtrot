package com.flipkart.foxtrot.server.auth.filter;

import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.UserPrincipal;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Priorities;
import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

/**
 * This filter validates the token
 */
@Priority(Priorities.AUTHENTICATION)
@WebFilter("/*")
@Slf4j
public class UserAuthenticationFilter implements Filter {
    private static final Set<String> WHITELISTED_PATTERNS = ImmutableSet.<String>builder()
            .add("/foxtrot/oauth")
            .add("^/foxtrot/auth.*")
            .build();
    private final AuthConfig authConfig;
    private final Provider<Authenticator<String, UserPrincipal>> authenticator;

    @Inject
    public UserAuthenticationFilter(
            AuthConfig authConfig, Provider<Authenticator<String, UserPrincipal>> authenticator) {
        this.authConfig = authConfig;
        this.authenticator = authenticator;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(
            ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (!authConfig.isEnabled()) {
            log.trace("Auth disabled");
            chain.doFilter(request, response);
            return;
        }
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        final String requestURI = httpRequest.getRequestURI();
        if (WHITELISTED_PATTERNS.stream().anyMatch(requestURI::startsWith)) {
            chain.doFilter(request, response);
            return;
        }
        val jwt = getTokenFromCookieOrHeader(httpRequest).orElse(null);
        if (!Strings.isNullOrEmpty(jwt)) {
            try {
                val principal = authenticator.get()
                        .authenticate(jwt).orElse(null);
                if (null != principal) {
                    SessionUser.put(principal);
                    chain.doFilter(request, response);
                    return;
                } else {
                    log.info("No principal ");
                }
            } catch (AuthenticationException e) {
                log.error("Jwt validation failure: ", e);
            }
        } else {
            log.debug("No token in request");
        }
        val referrer = httpRequest.getHeader(org.apache.http.HttpHeaders.REFERER);
        val source = Strings.isNullOrEmpty(referrer) ? requestURI : referrer;
        httpResponse.addCookie(new Cookie("redirection", source));
        httpResponse.sendRedirect("/foxtrot/oauth/login");
    }

    @Override
    public void destroy() {

    }

    private Optional<String> getTokenFromCookieOrHeader(HttpServletRequest servletRequest) {
        val tokenFromHeader = getTokenFromHeader(servletRequest);
        return tokenFromHeader.isPresent() ? tokenFromHeader : getTokenFromCookie(servletRequest);
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
            val token = Arrays.stream(cookies).filter(cookie -> cookie.getName().equals("token")).findAny().orElse(null);
            if (null != token) {
                return Optional.of(token.getValue());
            }
        }
        return Optional.empty();
    }

}

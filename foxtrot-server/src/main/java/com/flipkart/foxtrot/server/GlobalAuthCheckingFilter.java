package com.flipkart.foxtrot.server;

import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.sessionstore.SessionDataStore;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
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
import java.util.Set;

/**
 *
 */
@Priority(Priorities.AUTHENTICATION)
@WebFilter("/*")
@Slf4j
public class GlobalAuthCheckingFilter implements Filter {
    private static final Set<String> WHITELISTED_PATTERNS = ImmutableSet.<String>builder()
            .add("/foxtrot/google")
            .add("^/foxtrot/auth.*")
        .build();
    private final AuthConfig authConfig;
    private final Provider<SessionDataStore> sessionDataStore;

    @Inject
    public GlobalAuthCheckingFilter(
            AuthConfig authConfig,
            Provider<SessionDataStore> sessionDataStore) {
        this.authConfig = authConfig;
        this.sessionDataStore = sessionDataStore;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(
            ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if(!authConfig.isEnabled()) {
            log.trace("Auth disabled");
            chain.doFilter(request, response);
            return;
        }
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        final String requestURI = httpRequest.getRequestURI();
        if(WHITELISTED_PATTERNS.stream().anyMatch(requestURI::startsWith)) {
            chain.doFilter(request, response);
            return;
        }
        val authHeader = httpRequest.getHeader(HttpHeaders.AUTHORIZATION);
        val cookies = httpRequest.getCookies();
        if(!Strings.isNullOrEmpty(authHeader)) {
            chain.doFilter(request, response);
            return;
        }
        if(null != cookies && cookies.length != 0
                && Arrays.stream(cookies).anyMatch(cookie -> cookie.getName().equals("token"))) {
            chain.doFilter(request, response);
            return;
        }
        val referrer = httpRequest.getHeader(org.apache.http.HttpHeaders.REFERER);
        val source = Strings.isNullOrEmpty(referrer) ? requestURI : referrer;
        httpResponse.addCookie(new Cookie("redirection", source));
        httpResponse.sendRedirect("/foxtrot/google/login");
    }

    @Override
    public void destroy() {

    }
}

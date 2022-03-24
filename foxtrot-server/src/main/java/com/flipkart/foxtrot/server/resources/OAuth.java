package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.authprovider.AuthProvider;
import com.flipkart.foxtrot.server.auth.authprovider.IdType;
import com.flipkart.foxtrot.server.auth.authprovider.impl.GoogleAuthProvider;
import com.flipkart.foxtrot.server.auth.sessionstore.SessionDataStore;
import com.flipkart.foxtrot.server.utils.AuthUtils;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpHeaders;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.UUID;

/**
 *
 */
@Path("/oauth")
@Slf4j
public class OAuth {

    private final AuthConfig authConfig;
    private final Provider<AuthProvider> authProvider;
    private final Provider<SessionDataStore> sessionDataStore;

    @Inject
    public OAuth(
            AuthConfig authConfig,
            Provider<AuthProvider> authProvider,
            Provider<SessionDataStore> sessionDataStore) {
        this.authConfig = authConfig;
        this.authProvider = authProvider;
        this.sessionDataStore = sessionDataStore;
    }

    @GET
    @Path("/login")
    public Response login(
            @CookieParam("redirection") final Cookie cookieReferrer,
            @HeaderParam(HttpHeaders.REFERER) final String referrer) {
        final String sessionId = UUID.randomUUID().toString();
        final String redirectionURL = authProvider.get()
                .redirectionURL(sessionId);
        log.info("Redirection uri: {}", redirectionURL);
        final String cookieReferrerUrl = null == cookieReferrer
                ? null
                : cookieReferrer.getValue();
        val source = Strings.isNullOrEmpty(cookieReferrerUrl)
                ? referrer
                : cookieReferrerUrl;
        log.debug("Call source: {} Referrer: {} Redirection: {}", source, referrer, cookieReferrerUrl);
        if (!Strings.isNullOrEmpty(source)) {
            sessionDataStore.get()
                    .put(sessionId, source);
            log.debug("Saved: {} against session: {}", source, sessionId);
        }
        return Response.seeOther(URI.create(redirectionURL))
                .cookie(
                        new NewCookie(
                                "gauth-state",
                                sessionId,
                                GoogleAuthProvider.CALLBACK_PATH,
                                null,
                                Cookie.DEFAULT_VERSION,
                                null,
                                NewCookie.DEFAULT_MAX_AGE,
                                null,
                                false,
                                false))
                .build();
    }

    @GET
    @Path("/callback")
    public Response handleGoogleCallback(
            @CookieParam("gauth-state") final Cookie cookieState,
            @Context HttpServletRequest requestContext,
            @QueryParam("state") final String sessionId,
            @QueryParam("code") final String authCode) {
        log.info("Request Ctx: {}", requestContext);
        if (null == cookieState
                || !cookieState.getValue().equals(sessionId)) {
            return Response.seeOther(URI.create("/"))
                    .cookie(expireCookie(cookieState))
                    .build();
        }
        val token = authProvider.get()
                .login(authCode, sessionId)
                .orElse(null);
        if (null == token) {
            return Response.seeOther(URI.create("/foxtrot/oauth/login")).build();
        }
        val existingReferrer = sessionDataStore.get()
                .get(token.getId())
                .orElse(null);
        if (null != existingReferrer) {
            sessionDataStore.get()
                    .delete(token.getId());
        }
        log.debug("Got: {} against session: {}", existingReferrer, authCode);
        final String finalRedirect = Strings.isNullOrEmpty((String) existingReferrer)
                ? "/"
                : (String) existingReferrer;
        log.debug("Will be redirecting to: {}. Existing: {}", finalRedirect, existingReferrer);
        return Response.seeOther(URI.create(finalRedirect))
                .cookie(createTokenCookie(token.getIdType().equals(IdType.SESSION_ID)
                                ? AuthUtils.createJWT(token, authConfig.getJwt())
                                : token.getId()),
                        expireCookie(cookieState))
                .build();
    }

    @GET
    @Path("/logout")
//    @PermitAll
    public Response logout(@CookieParam("token") final Cookie token) {
        if (null != token) {
            val jwt = token.getValue();
            log.debug("Token value: {}", jwt);
            val status = authProvider.get().logout(jwt);
            log.info("Token deletion status for token {} is {}", jwt, status);
            return Response.seeOther(URI.create("/"))
                    .cookie(createTokenCookie(""))
                    .build();
        } else {
            log.info("Empty token in logout call");
        }
        return Response.seeOther(URI.create("/")).build();
    }

    private NewCookie createTokenCookie(String value) {
        return new NewCookie("token",
                value,
                "/",
                null,
                Cookie.DEFAULT_VERSION,
                null,
                NewCookie.DEFAULT_MAX_AGE,
                null,
                false,
                true);
    }

    private NewCookie expireCookie(@CookieParam("gauth-state") Cookie cookieState) {
        return new NewCookie(cookieState, null, 0, null, false, true);
    }

}

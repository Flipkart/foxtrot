package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.sessionstore.SessionDataStore;
import com.flipkart.foxtrot.server.auth.authprovider.AuthProvider;
import com.flipkart.foxtrot.server.auth.authprovider.impl.GoogleAuthProvider;
import com.flipkart.foxtrot.server.utils.AuthUtils;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpHeaders;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.net.URI;
import java.util.UUID;

/**
 *
 */
@Path("/google")
@Slf4j
public class GoogleAuth {

    private final AuthConfig authConfig;
    private final Provider<AuthProvider> authProvider;
    private final Provider<SessionDataStore> sessionDataStore;

    @Inject
    public GoogleAuth(
            AuthConfig authConfig,
            Provider<AuthProvider> authProvider,
            Provider<SessionDataStore> sessionDataStore) {
        this.authConfig = authConfig;
        this.authProvider = authProvider;
        this.sessionDataStore = sessionDataStore;
    }

    @GET
    @Path("/login")
    public Response login(@CookieParam("redirection") final Cookie cookieReferrer,
                          @HeaderParam(HttpHeaders.REFERER) final String referrer) {
        final String sessionId = UUID.randomUUID().toString();
        final String redirectionURL = authProvider.get()
                .redirectionURL(sessionId);
        log.info("Redirection uri: {}", redirectionURL);
        final String cookieReferrerUrl = null == cookieReferrer ? null : cookieReferrer.getValue();
        val source = Strings.isNullOrEmpty(cookieReferrerUrl) ?  referrer : cookieReferrerUrl;
        log.info("Call source: {} Referrer: {} Redirection: {}", source, referrer, cookieReferrerUrl);
        if(!Strings.isNullOrEmpty(source)) {
            sessionDataStore.get()
                    .put(sessionId, source);
            log.info("Saved: {} against session: {}", source, sessionId);
        }
        return Response.seeOther(URI.create(redirectionURL))
                .cookie(
                        new NewCookie(
                                "gauth-state",
                                sessionId,
                                GoogleAuthProvider.CALLBACK_PATH,
                                null,
                                NewCookie.DEFAULT_VERSION,
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
                    .cookie(new NewCookie(cookieState, null, 0, false))
                    .build();
        }
        val token = authProvider.get()
                .login(authCode, sessionId)
                .orElse(null);
        if (null == token) {
            return Response.seeOther(URI.create("/foxtrot/login/google")).build();
        }
        val existingReferrer = sessionDataStore.get()
                .get(token.getId())
                .orElse(null);
        if(null != existingReferrer) {
            sessionDataStore.get()
                    .delete(token.getId());
        }
        log.info("Got: {} against session: {}", existingReferrer, authCode);
        final String finalRedirect = Strings.isNullOrEmpty((String)existingReferrer) ? "/" : (String)existingReferrer;
        log.info("Will be redirecting to: {}. Existing: {}", finalRedirect, existingReferrer);
        return Response.seeOther(URI.create(finalRedirect))
                .cookie(new NewCookie("token",
                                      AuthUtils.createJWT(token, authConfig.getJwt()),
                                      "/",
                                      null,
                                      Cookie.DEFAULT_VERSION,
                                      null,
                                      NewCookie.DEFAULT_MAX_AGE,
                                      null,
                                      false,
                                      true),
                        new NewCookie(cookieState, null, 0, false))
                .build();
    }
}

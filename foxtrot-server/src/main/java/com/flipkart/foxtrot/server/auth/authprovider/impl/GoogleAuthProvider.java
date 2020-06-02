package com.flipkart.foxtrot.server.auth.authprovider.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.AuthStore;
import com.flipkart.foxtrot.server.auth.Token;
import com.flipkart.foxtrot.server.auth.TokenType;
import com.flipkart.foxtrot.server.auth.authprovider.AuthProvider;
import com.flipkart.foxtrot.server.auth.authprovider.AuthType;
import com.flipkart.foxtrot.server.utils.AuthUtils;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.dropwizard.util.Duration;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Date;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 *
 */
@Slf4j
public class GoogleAuthProvider implements AuthProvider {

    public static final String CALLBACK_PATH = "/foxtrot/google/callback";

    private final HttpTransport transport;
    private final GoogleAuthorizationCodeFlow authorizationCodeFlow;
    private final String redirectionUrl;
    private final AuthConfig authConfig;
    private final GoogleAuthProviderConfig googleAuthConfig;
    private final ObjectMapper mapper;
    private final AuthStore credentialsStorage;

    @Inject
    public GoogleAuthProvider(GoogleAuthProviderConfig googleAuthConfig, AuthConfig authConfig, ObjectMapper mapper,
            AuthStore credentialsStorage) {
        this.authConfig = authConfig;
        final NetHttpTransport.Builder transportBuilder = new NetHttpTransport.Builder();
        Proxy proxy = Proxy.NO_PROXY;
        if (googleAuthConfig.getProxyType() != null) {
            switch (googleAuthConfig.getProxyType()) {
                case DIRECT:
                    break;
                case HTTP: {
                    Preconditions.checkArgument(!Strings.isNullOrEmpty(googleAuthConfig.getProxyHost()));
                    proxy = new Proxy(Proxy.Type.HTTP,
                            new InetSocketAddress(googleAuthConfig.getProxyHost(), googleAuthConfig.getProxyPort()));
                    break;
                }
                case SOCKS:
                    Preconditions.checkArgument(!Strings.isNullOrEmpty(googleAuthConfig.getProxyHost()));
                    proxy = new Proxy(Proxy.Type.HTTP,
                            new InetSocketAddress(googleAuthConfig.getProxyHost(), googleAuthConfig.getProxyPort()));
                    break;
            }
        }
        this.transport = transportBuilder.setProxy(proxy)
                .build();
        this.authorizationCodeFlow = new GoogleAuthorizationCodeFlow.Builder(transport, new JacksonFactory(),
                googleAuthConfig.getClientId(), googleAuthConfig.getClientSecret(),
                ImmutableSet.of("https://www.googleapis.com/auth/userinfo.email")).build();
        this.redirectionUrl =
                (googleAuthConfig.isSecureEndpoint() ? "https" : "http") + "://" + googleAuthConfig.getServer()
                        + CALLBACK_PATH;
        this.googleAuthConfig = googleAuthConfig;
        this.mapper = mapper;
        this.credentialsStorage = credentialsStorage;
    }

    @Override
    public AuthType type() {
        return AuthType.OAUTH_GOOGLE;
    }

    @Override
    public String redirectionURL(String sessionId) {
        final String url = authorizationCodeFlow.newAuthorizationUrl()
                .setState(sessionId)
                .setRedirectUri(this.redirectionUrl)
//                .setRedirectUri("http://localhost:8080/auth/google")
                .build();
        return !Strings.isNullOrEmpty(googleAuthConfig.getLoginDomain()) ? (url + "&hd="
                + googleAuthConfig.getLoginDomain()) : url;
    }

    @Override
    public Optional<Token> login(String authToken, String sessionId) {
        if (Strings.isNullOrEmpty(authToken)) {
            return Optional.empty();
        }
        final GoogleAuthorizationCodeTokenRequest googleAuthorizationCodeTokenRequest = authorizationCodeFlow.newTokenRequest(
                authToken);
        final String email;
        try {
            final GoogleTokenResponse tokenResponse = googleAuthorizationCodeTokenRequest.setRedirectUri(
                    this.redirectionUrl)
                    .execute();
            final Credential credential = authorizationCodeFlow.createAndStoreCredential(tokenResponse, null);
            final HttpRequestFactory requestFactory = transport.createRequestFactory(credential);
            // Make an authenticated request
            final GenericUrl url = new GenericUrl("https://www.googleapis.com/oauth2/v1/userinfo");
            final HttpRequest request = requestFactory.buildGetRequest(url);
            request.getHeaders()
                    .setContentType("application/json");
            final String jsonIdentity = request.execute()
                    .parseAsString();
            log.debug("Identity: {}", jsonIdentity);
            email = mapper.readTree(jsonIdentity)
                    .get("email")
                    .asText();
        } catch (IOException e) {
            log.error("Error logging in using google:", e);
            return Optional.empty();
        }
        val user = credentialsStorage.getUser(email)
                .orElse(null);
        if (null == user) {
            log.warn("No authorized user found for email: {}", email);
            return Optional.empty();
        }
        final Duration sessionDuration = AuthUtils.sessionDuration(authConfig);
        return credentialsStorage.provisionToken(user.getId(), sessionId, TokenType.DYNAMIC,
                new Date(new Date().getTime() + sessionDuration.toMilliseconds()));
    }

    @Override
    public boolean isPreregistrationRequired() {
        return false;
    }

}

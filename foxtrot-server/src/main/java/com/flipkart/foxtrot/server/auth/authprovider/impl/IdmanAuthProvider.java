package com.flipkart.foxtrot.server.auth.authprovider.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import com.flipkart.foxtrot.core.auth.User;
import com.flipkart.foxtrot.server.auth.*;
import com.flipkart.foxtrot.server.auth.authprovider.AuthProvider;
import com.flipkart.foxtrot.server.auth.authprovider.AuthType;
import com.flipkart.foxtrot.server.auth.authprovider.IdType;
import com.google.common.collect.ImmutableList;
import io.appform.idman.model.IdmanUser;
import io.appform.idman.model.TokenInfo;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

/**
 *
 */
@Slf4j
public class IdmanAuthProvider implements AuthProvider {
    private final CloseableHttpClient client = HttpClients.createDefault();
    private final IdmanAuthProviderConfig config;
    private final ObjectMapper mapper;

    public IdmanAuthProvider(IdmanAuthProviderConfig config, ObjectMapper mapper) {
        this.config = config;
        this.mapper = mapper;

    }

    @Override
    public AuthType type() {
        return AuthType.OAUTH_IDMAN;
    }

    @Override
    @SneakyThrows
    public String redirectionURL(String sessionId) {
        return new URIBuilder(config.getIdmanEndpoint())
                .setPath("/apis/oauth2/authorize")
                .addParameter("response_type", "code")
                .addParameter("client_id", config.getClientId())
                .addParameter("state", sessionId)
                .addParameter("redirect_uri", config.getServerEndpoint() + "/foxtrot/oauth/callback")
                .build()
                .toString();
    }

    @Override
    @SneakyThrows
    public Optional<Token> login(String authCode, String sessionId) {
        return callIdmanServer("authorization_code", authCode,
                               ti -> {
                                   val idmanUser = ti.getUser();
                                   val expiry = Date.from(Instant.now().plusSeconds(ti.getExpiry()));
                                   return Optional.of(new Token(ti.getAccessToken(),
                                                                IdType.ACCESS_TOKEN,
                                                                TokenType.DYNAMIC,
                                                                idmanUser.getUser().getId(),
                                                                expiry));
                               });
    }

    @Override
    @SneakyThrows
    public Optional<AuthenticatedInfo> authenticate(AuthInfo authInfo) {
        return refreshToken(authInfo.accept(TokenAuthInfo::getToken));
    }

    @SneakyThrows
    private Optional<AuthenticatedInfo> refreshToken(String token) {
        return callIdmanServer(
                "refresh_token",
                token,
                ti -> {
                    val idmanUser = ti.getUser();
                    val expiry = Date.from(Instant.now().plusSeconds(ti.getExpiry()));
                    val remoteUser = idmanUser.getUser();
                    val t = new Token(ti.getAccessToken(),
                                      IdType.ACCESS_TOKEN,
                                      TokenType.DYNAMIC,
                                      remoteUser.getId(),
                                      expiry);
                    val user = new User(remoteUser.getId(),
                                        mapToFoxtrotRoles(idmanUser),
                                        Collections.emptySet(),
                                        isSystemUser(idmanUser),
                                        new Date(),
                                        new Date());
                    return Optional.of(new AuthenticatedInfo(t, user));
                });
    }

    private Set<FoxtrotRole> mapToFoxtrotRoles(final IdmanUser idmanUser) {
        switch (idmanUser.getRole()) {
            case "FOXTROT_ADMIN": {
                return EnumSet.allOf(FoxtrotRole.class);
            }
            case "FOXTROT_HUMAN_USER": {
                return EnumSet.of(FoxtrotRole.CONSOLE, FoxtrotRole.QUERY, FoxtrotRole.INGEST);
            }
            case "FOXTROT_SYSTEM_USER": {
                return EnumSet.of(FoxtrotRole.QUERY, FoxtrotRole.INGEST);
            }
            default:
                throw new IllegalStateException("Unexpected value: " + idmanUser.getRole());
        }
    }

    private boolean isSystemUser(final IdmanUser user) {
        return user.getRole().equals("FOXTROT_ADMIN");
    }

    private <T> Optional<T> callIdmanServer(
            String callMode,
            String param,
            Function<TokenInfo, Optional<T>> handler) throws URISyntaxException, IOException {
        val post = new HttpPost(new URIBuilder(config.getIdmanEndpoint())
                                        .setPath("/apis/oauth2/token")
                                        .build());
        val fromBuilder = ImmutableList.<NameValuePair>builder();
        val form = fromBuilder
                .add(new BasicNameValuePair(callMode.equals("authorization_code")
                                            ? "code"
                                            : "refresh_token", param))
                .add(new BasicNameValuePair("client_id", config.getClientId()))
                .add(new BasicNameValuePair("client_secret", config.getClientSecret()))
                .add(new BasicNameValuePair("grant_type", callMode))
                .build();
        post.setEntity(new UrlEncodedFormEntity(form));
        try (val response = client.execute(post)) {
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                log.error("Error calling {}: {}", post.getURI().toString(), response.getStatusLine());
                return Optional.empty();
            }
            val ti = mapper.readValue(EntityUtils.toByteArray(response.getEntity()), TokenInfo.class);
            return handler.apply(ti);
        }
    }

}

package com.flipkart.foxtrot.server.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.auth.User;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import io.dropwizard.util.Duration;
import java.util.Date;
import java.util.Optional;
import java.util.function.UnaryOperator;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.rest.RestStatus;

/**
 *
 */
@Slf4j
public class ESAuthStore implements AuthStore {

    private final ElasticsearchConnection connection;
    private final ObjectMapper mapper;

    @Inject
    public ESAuthStore(ElasticsearchConnection connection,
                       ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    @SneakyThrows
    public Optional<User> provisionUser(User user) {
        return Optional.of(null);
    }

    @Override
    @SneakyThrows
    public Optional<User> getUser(String userId) {
        return Optional.of(null);
    }

    @SneakyThrows
    @Override
    public boolean deleteUser(String id) {
        return true;
    }

    @Override
    @SneakyThrows
    public boolean updateUser(String id,
                              UnaryOperator<User> mutator) {
        return true;
    }

    @Override
    @SneakyThrows
    public Optional<Token> provisionToken(String userId,
                                          String tokenId,
                                          TokenType tokenType,
                                          Date expiry) {
        return Optional.of(null);
    }

    @Override
    @SneakyThrows
    public Optional<Token> getToken(String tokenId) {
        return Optional.of(null);
    }

    @Override
    @SneakyThrows
    public Optional<Token> getTokenForUser(String userId) {
        return Optional.of(null);
    }

    @Override
    @SneakyThrows
    public boolean deleteToken(String tokenId) {
        return true;
    }

    @Override
    @SneakyThrows
    public boolean deleteExpiredTokens(Date date,
                                       Duration sessionDuration) {
        return true;
    }


    @SneakyThrows
    private RestStatus saveUser(User user,
                                DocWriteRequest.OpType opType) {
        return RestStatus.ACCEPTED;
    }

    @SneakyThrows
    public long deleteTokensForUser(String userId) {
        return 1;
    }
}

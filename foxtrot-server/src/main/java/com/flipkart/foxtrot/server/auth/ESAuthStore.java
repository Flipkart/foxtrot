package com.flipkart.foxtrot.server.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.auth.User;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import lombok.SneakyThrows;
import lombok.val;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import javax.inject.Inject;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 *
 */

public class ESAuthStore implements AuthStore {
    private static final String USERS_INDEX = "user-meta";
    private static final String TOKENS_INDEX = "tokens";

    private final ElasticsearchConnection connection;
    private final ObjectMapper mapper;

    @Inject
    public ESAuthStore(ElasticsearchConnection connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    @SneakyThrows
    public Optional<User> provision(User user) {
        val status = saveUser(user, DocWriteRequest.OpType.CREATE);
        if (status != RestStatus.OK) {
            return Optional.empty();
        }
        return getUser(user.getId());
    }

    @Override
    @SneakyThrows
    public Optional<User> getUser(String userId) {
        val getResp = connection.getClient()
                .get(new GetRequest(USERS_INDEX)
                             .id(userId))
                .actionGet();
        if (!getResp.isExists()) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(getResp.getSourceAsString(), User.class));
    }

    @Override
    public boolean deleteUser(String id) {
        return connection.getClient()
                .delete(new DeleteRequest(USERS_INDEX)
                                .id(id)
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .actionGet()
                .status() == RestStatus.OK;
    }

    @Override
    @SneakyThrows
    public boolean updateUser(
            String id, UnaryOperator<User> mutator) {
        val user = getUser(id).orElse(null);
        if (null == user) {
            return false;
        }
        final User updatedUser = mutator.apply(user);
        return saveUser(updatedUser, DocWriteRequest.OpType.UPDATE) == RestStatus.OK;
    }

    @Override
    @SneakyThrows
    public Optional<Token> provisionToken(String userId, TokenType tokenType, Date expiry) {
        val userPresent = getUser(userId).isPresent();
        if (!userPresent) {
            return Optional.empty();
        }
        final String tokenId = UUID.randomUUID().toString();
        val saveStatus = connection.getClient()
                .index(new IndexRequest(TOKENS_INDEX)
                               .source(mapper.writeValueAsString(new Token(tokenId, tokenType, userId, expiry)), XContentType.JSON)
                               .id(userId)
                               .type("TOKEN")
                               .create(true)
                               .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .actionGet()
                .status();
        if (saveStatus != RestStatus.OK) {
            return Optional.empty();
        }
        return getToken(tokenId);
    }

    @Override
    @SneakyThrows
    public Optional<Token> getToken(String tokenId) {
        val getResp = connection.getClient()
                .get(new GetRequest(TOKENS_INDEX)
                             .id(tokenId))
                .actionGet();
        if (!getResp.isExists()) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(getResp.getSourceAsString(), Token.class));
    }

    @Override
    public boolean deleteToken(String tokenId) {
        return connection.getClient()
                .delete(new DeleteRequest(TOKENS_INDEX)
                                .id(tokenId)
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .actionGet()
                .status() == RestStatus.OK;
    }


    private RestStatus saveUser(User user, DocWriteRequest.OpType opType) throws JsonProcessingException {
        return connection.getClient()
                .index(new IndexRequest(USERS_INDEX)
                               .source(mapper.writeValueAsString(user), XContentType.JSON)
                               .id(user.getId())
                               .type("USER")
                               .opType(opType)
                               .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .actionGet()
                .status();
    }
}

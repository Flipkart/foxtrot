package com.flipkart.foxtrot.server.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.auth.User;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.inject.Inject;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 *
 */
@Slf4j
public class ESAuthStore implements AuthStore {
    public static final String USERS_INDEX = "user-meta";
    public static final String TOKENS_INDEX = "tokens";
    private static final String TOKEN_TYPE = "TOKEN";
    private static final String USER_TYPE = "USER";

    private final ElasticsearchConnection connection;
    private final ObjectMapper mapper;

    @Inject
    public ESAuthStore(ElasticsearchConnection connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    @SneakyThrows
    public Optional<User> provisionUser(User user) {
        val status = saveUser(user, DocWriteRequest.OpType.CREATE);
        if (status != RestStatus.CREATED) {
            return Optional.empty();
        }
        return getUser(user.getId());
    }

    @Override
    @SneakyThrows
    public Optional<User> getUser(String userId) {
        val getResp = connection.getClient()
                .get(new GetRequest(USERS_INDEX, USER_TYPE, userId), RequestOptions.DEFAULT);
        if (!getResp.isExists()) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(getResp.getSourceAsString(), User.class));
    }

    @SneakyThrows
    @Override
    public boolean deleteUser(String id) {
        return connection.getClient()
                .delete(new DeleteRequest(USERS_INDEX, USER_TYPE, id)
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT)
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
        return saveUser(updatedUser, DocWriteRequest.OpType.INDEX) == RestStatus.OK;
    }

    @Override
    @SneakyThrows
    public Optional<Token> provisionToken(String userId, TokenType tokenType, Date expiry) {
        val userPresent = getUser(userId).isPresent();
        if (!userPresent) {
            log.warn("No user found for is: {}", userId);
            return Optional.empty();
        }
        final String tokenId = UUID.randomUUID().toString();
        try {
            val saveStatus = connection.getClient()
                    .index(new IndexRequest(TOKENS_INDEX)
                                   .source(mapper.writeValueAsString(new Token(tokenId, tokenType, userId, expiry)),
                                           XContentType.JSON)
                                   .id(tokenId)
                                   .type(TOKEN_TYPE)
                                   .create(true)
                                   .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT)
                    .status();
            if (saveStatus != RestStatus.CREATED) {
                log.error("ES save status for token for user {} is: {}", userId, saveStatus);
                return Optional.empty();
            }
        } catch (ElasticsearchException v) {
            log.warn("A valid token exists exists already.. for id: {}", userId);
            return getTokenForUser(userId);
        }
        return getToken(tokenId);
    }

    @Override
    @SneakyThrows
    public Optional<Token> getToken(String tokenId) {
        val getResp = connection.getClient()
                .get(new GetRequest(TOKENS_INDEX, TOKEN_TYPE, tokenId), RequestOptions.DEFAULT);
        if (!getResp.isExists()) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(getResp.getSourceAsString(), Token.class));
    }

    @Override
    @SneakyThrows
    public Optional<Token> getTokenForUser(String userId) {
        val getResp = connection.getClient()
                .search(new SearchRequest(TOKENS_INDEX)
                                .searchType(SearchType.QUERY_THEN_FETCH)
                                .types(TOKEN_TYPE)
                                .source(new SearchSourceBuilder().query(
                                        QueryBuilders.termQuery("userId", userId))),
                        RequestOptions.DEFAULT);
        final SearchHits hits = getResp.getHits();
        if (hits.totalHits <= 0) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(hits.getAt(0).getSourceAsString(), Token.class));
    }

    @Override
    @SneakyThrows
    public boolean deleteToken(String tokenId) {
        return connection.getClient()
                .delete(new DeleteRequest(TOKENS_INDEX, TOKEN_TYPE, tokenId)
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT)
                .status() == RestStatus.OK;
    }


    @SneakyThrows
    private RestStatus saveUser(User user, DocWriteRequest.OpType opType) {
        return connection.getClient()
                .index(new IndexRequest(USERS_INDEX)
                               .source(mapper.writeValueAsString(user), XContentType.JSON)
                               .id(user.getId())
                               .type(USER_TYPE)
                               .opType(opType)
                               .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                       RequestOptions.DEFAULT)
                .status();
    }
}

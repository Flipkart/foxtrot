package com.flipkart.foxtrot.server.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.auth.User;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import com.flipkart.foxtrot.server.auth.authprovider.IdType;
import io.dropwizard.util.Duration;
import java.util.Date;
import java.util.Optional;
import java.util.function.UnaryOperator;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 *
 */
@Slf4j
public class ESAuthStore implements AuthStore {
    public static final String USERS_INDEX = "user-meta";
    public static final String TOKENS_INDEX = "tokens";
    private static final String TOKEN_TYPE = "TOKEN";
    private static final String USER_TYPE = "USER";

    private final OpensearchConnection connection;
    private final ObjectMapper mapper;

    @Inject
    public ESAuthStore(OpensearchConnection connection,
                       ObjectMapper mapper) {
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
                .get(new GetRequest(USERS_INDEX, userId), RequestOptions.DEFAULT);
        if (!getResp.isExists()) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(getResp.getSourceAsString(), User.class));
    }

    @SneakyThrows
    @Override
    public boolean deleteUser(String userId) {
        boolean status = connection.getClient()
                .delete(new DeleteRequest(USERS_INDEX, userId).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                        RequestOptions.DEFAULT)
                .status() == RestStatus.OK;
        if (status) {
            val count = deleteTokensForUser(userId);
            log.debug("User {} deleted and {} existing tokens deleted with that.", userId, count);
        }
        return status;
    }

    @Override
    @SneakyThrows
    public boolean updateUser(
            String userId, UnaryOperator<User> mutator) {
        val user = getUser(userId).orElse(null);
        if (null == user) {
            return false;
        }
        final User updatedUser = mutator.apply(user);
        return saveUser(updatedUser, DocWriteRequest.OpType.INDEX) == RestStatus.OK;
    }

    @Override
    @SneakyThrows
    public Optional<Token> provisionToken(String userId, String tokenId, TokenType tokenType, Date expiry) {
        val user = getUser(userId).orElse(null);
        if (null == user) {
            log.warn("No user found for is: {}", userId);
            return Optional.empty();
        }
        if ((tokenType.equals(TokenType.STATIC) || tokenType.equals(TokenType.SYSTEM)) && !user.isSystemUser()) {
            log.warn("Cannot create system/static token for non system user");
            return Optional.empty();
        }

        val saveStatus = connection.getClient()
                .index(new IndexRequest(TOKENS_INDEX).source(
                                mapper.writeValueAsString(new Token(tokenId, IdType.SESSION_ID, tokenType, userId, expiry)),
                                XContentType.JSON)
                        .id(tokenId)
                        .create(true)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT)
                .status();
        if (saveStatus != RestStatus.CREATED) {
            log.error("ES save status for token for user {} is: {}", userId, saveStatus);
            return Optional.empty();
        }
        log.info("Token created for user: {}", userId);

        return getToken(tokenId);
    }

    @Override
    @SneakyThrows
    public Optional<Token> getToken(String tokenId) {
        val getResp = connection.getClient()
                .get(new GetRequest(TOKENS_INDEX, tokenId), RequestOptions.DEFAULT);
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
                                .source(new SearchSourceBuilder().query(
                                        QueryBuilders.termQuery("userId", userId))),
                        RequestOptions.DEFAULT);
        final SearchHits hits = getResp.getHits();
        if (hits.getHits().length <= 0) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(hits.getAt(0).getSourceAsString(), Token.class));
    }

    @Override
    @SneakyThrows
    public boolean deleteToken(String tokenId) {
        return connection.getClient()
                .delete(new DeleteRequest(TOKENS_INDEX, tokenId).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                        RequestOptions.DEFAULT)
                .status() == RestStatus.OK;
    }

    @Override
    @SneakyThrows
    public boolean deleteExpiredTokens(Date date, Duration sessionDuration) {
        log.info("Cleaning up sessions older than: {}", sessionDuration);
        Date oldestValidDate = new Date(date.getTime() - sessionDuration.toMilliseconds());
        val deletedCount = connection.getClient()
                .deleteByQuery(new DeleteByQueryRequest(TOKENS_INDEX)
                        .setIndicesOptions(Utils.indicesOptions())
                        .setQuery(QueryBuilders.rangeQuery("expiry")
                                .lt(oldestValidDate.getTime()))
                        .setRefresh(true), RequestOptions.DEFAULT)
                .getDeleted();
        log.info("Deleted {} expired tokens", deletedCount);
        return true;
    }


    @SneakyThrows
    private RestStatus saveUser(User user, DocWriteRequest.OpType opType) {
        return connection.getClient()
                .index(new IndexRequest(USERS_INDEX)
                                .source(mapper.writeValueAsString(user), XContentType.JSON)
                                .id(user.getId())
                                .opType(opType)
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                        RequestOptions.DEFAULT)
                .status();
    }

    @SneakyThrows
    public long deleteTokensForUser(String userId) {
        return connection.getClient()
                .deleteByQuery(new DeleteByQueryRequest(TOKENS_INDEX)
                                .setQuery(QueryBuilders.termQuery("userId", userId))
                                .setRefresh(true),
                        RequestOptions.DEFAULT)
                .getDeleted();
    }
}

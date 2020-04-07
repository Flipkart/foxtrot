package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import com.flipkart.foxtrot.core.auth.User;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.google.common.collect.Sets;
import io.dropwizard.jackson.Jackson;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;

/**
 *
 */
public class ESAuthStoreTest {

    private ElasticsearchConnection elasticsearchConnection;
    private AuthStore authStore;

    @Before
    public void setup() throws Exception {
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        TestUtils.ensureIndex(elasticsearchConnection, ESAuthStore.USERS_INDEX);
        TestUtils.ensureIndex(elasticsearchConnection, ESAuthStore.TOKENS_INDEX);
        authStore = new ESAuthStore(elasticsearchConnection, Jackson.newObjectMapper());
    }

    @Test
    @SneakyThrows
    public void usersTest() {
        final String TEST_USER = "test_user";
        final User originalUser = new User(TEST_USER,
                                           EnumSet.of(FoxtrotRole.INGEST),
                                           Collections.emptySet(),
                                           null,
                                           null);
        Assert.assertEquals(originalUser, authStore.provisionUser(originalUser).orElse(null));
        Assert.assertEquals(originalUser, authStore.getUser(TEST_USER).orElse(null));
        Assert.assertTrue(authStore.grantRole(TEST_USER, FoxtrotRole.QUERY));
        Assert.assertTrue(Sets.difference(EnumSet.of(FoxtrotRole.INGEST, FoxtrotRole.QUERY),
                                          Objects.requireNonNull(authStore.getUser(TEST_USER)
                                                                         .map(User::getRoles)
                                                                         .orElse(null))).isEmpty());
        Assert.assertTrue(authStore.revokeRole(TEST_USER, FoxtrotRole.INGEST));
        Assert.assertTrue(Sets.difference(EnumSet.of(FoxtrotRole.QUERY),
                                          Objects.requireNonNull(authStore.getUser(TEST_USER)
                                                                         .map(User::getRoles)
                                                                         .orElse(null))).isEmpty());
        Assert.assertTrue(authStore.deleteUser(TEST_USER));
    }

    @Test
    public void tokensTest() {
        final String TEST_USER = "test_user";
        Assert.assertNull(authStore.provisionToken(TEST_USER, TokenType.STATIC, null)
                                  .orElse(null));
        final User originalUser = new User(TEST_USER,
                                           EnumSet.of(FoxtrotRole.INGEST),
                                           Collections.emptySet(), null, null);
        Assert.assertEquals(originalUser, authStore.provisionUser(originalUser).orElse(null));
        val token = authStore.provisionToken(TEST_USER, TokenType.STATIC, null)
                .orElse(null);
        Assert.assertNotNull(token);
        Assert.assertEquals(TEST_USER, token.getUserId());
        Assert.assertEquals(token.getId(),
                            Objects.requireNonNull(authStore.getToken(token.getId()).orElse(null)).getId());
        Assert.assertEquals(token.getId(),
                            Objects.requireNonNull(authStore.getTokenForUser(TEST_USER).orElse(null)).getId());
        Assert.assertTrue(authStore.deleteToken(token.getId()));
        Assert.assertNull(authStore.getToken(token.getId()).orElse(null));
        Assert.assertTrue(authStore.deleteUser(TEST_USER));
    }
}
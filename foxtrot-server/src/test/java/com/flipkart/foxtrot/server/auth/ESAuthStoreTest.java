package com.flipkart.foxtrot.server.auth;

/**
 *
 */
public class ESAuthStoreTest {

    /*private AuthStore authStore;
    private ElasticsearchRestConnection elasticsearchRestConnection;
    private ElasticsearchConnection elasticsearchConnection;

    @Before
    public void setup() throws Exception {
        elasticsearchRestConnection = ElasticsearchTestUtils.getRestConnection();
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        TestUtils.ensureIndex(elasticsearchConnection, ESAuthStore.USERS_INDEX);
        TestUtils.ensureIndex(elasticsearchConnection, ESAuthStore.TOKENS_INDEX);
        authStore = new ESAuthStore(elasticsearchRestConnection, Jackson.newObjectMapper());
    }

    @After
    public void teardown() {
        ElasticsearchTestUtils.cleanupIndices(elasticsearchConnection);
    }

    @Test
    @SneakyThrows
    public void usersTest() {
        final String TEST_USER = "test_user";
        final User originalUser = new User(TEST_USER, EnumSet.of(FoxtrotRole.INGEST), Collections.emptySet(), true,
                null, null);
        Assert.assertEquals(originalUser, authStore.provisionUser(originalUser)
                .orElse(null));
        Assert.assertEquals(originalUser, authStore.getUser(TEST_USER)
                .orElse(null));
        Assert.assertTrue(authStore.grantRole(TEST_USER, FoxtrotRole.QUERY));
        Assert.assertTrue(Sets.difference(EnumSet.of(FoxtrotRole.INGEST, FoxtrotRole.QUERY), Objects.requireNonNull(
                authStore.getUser(TEST_USER)
                        .map(User::getRoles)
                        .orElse(null)))
                .isEmpty());
        Assert.assertTrue(authStore.revokeRole(TEST_USER, FoxtrotRole.INGEST));
        Assert.assertTrue(Sets.difference(EnumSet.of(FoxtrotRole.QUERY), Objects.requireNonNull(
                authStore.getUser(TEST_USER)
                        .map(User::getRoles)
                        .orElse(null)))
                .isEmpty());
        Assert.assertTrue(authStore.deleteUser(TEST_USER));
    }

    @Test
    public void tokensTest() {
        final String TEST_USER = "test_user";
        Assert.assertNull(authStore.provisionToken(TEST_USER, TokenType.STATIC, null)
                .orElse(null));
        final User originalUser = new User(TEST_USER, EnumSet.of(FoxtrotRole.INGEST), Collections.emptySet(), true,
                null, null);
        Assert.assertEquals(originalUser, authStore.provisionUser(originalUser)
                .orElse(null));
        val token = authStore.provisionToken(TEST_USER, TokenType.STATIC, null)
                .orElse(null);
        Assert.assertNotNull(token);
        Assert.assertEquals(TEST_USER, token.getUserId());
        Assert.assertEquals(token.getId(), Objects.requireNonNull(authStore.getToken(token.getId())
                .orElse(null))
                .getId());
        Assert.assertEquals(token.getId(), Objects.requireNonNull(authStore.getTokenForUser(TEST_USER)
                .orElse(null))
                .getId());
        Assert.assertTrue(authStore.deleteToken(token.getId()));
        Assert.assertNull(authStore.getToken(token.getId())
                .orElse(null));
        Assert.assertTrue(authStore.deleteUser(TEST_USER));
    }

    @Test
    public void tokensDeleteTest() {
        final String TEST_USER = "test_user";
        Assert.assertNull(authStore.provisionToken(TEST_USER, TokenType.STATIC, null)
                .orElse(null));
        final User originalUser = new User(TEST_USER, EnumSet.of(FoxtrotRole.INGEST), Collections.emptySet(), true,
                null, null);
        Assert.assertEquals(originalUser, authStore.provisionUser(originalUser)
                .orElse(null));
        val token = authStore.provisionToken(TEST_USER, TokenType.STATIC, null)
                .orElse(null);
        Assert.assertNotNull(token);
        Assert.assertEquals(TEST_USER, token.getUserId());
        Assert.assertEquals(token.getId(), Objects.requireNonNull(authStore.getToken(token.getId())
                .orElse(null))
                .getId());
        Assert.assertEquals(token.getId(), Objects.requireNonNull(authStore.getTokenForUser(TEST_USER)
                .orElse(null))
                .getId());

        //Delete user and make sure tokens are deleted along with that
        Assert.assertTrue(authStore.deleteUser(TEST_USER));
        Assert.assertNull(authStore.getToken(token.getId())
                .orElse(null));
    }

    @Test
    public void expiredTokensDeleteTest() {
        val sessionDuration = Duration.seconds(2);
        final String TEST_USER = "test_user";
        final User originalUser = new User(TEST_USER, EnumSet.of(FoxtrotRole.INGEST), Collections.emptySet(), true,
                null, null);
        Assert.assertEquals(originalUser, authStore.provisionUser(originalUser)
                .orElse(null));
        final Date testDate = new Date();
        val tokenDate = new Date(testDate.getTime() - sessionDuration.toMilliseconds() - 5_000);
        val token = authStore.provisionToken(TEST_USER, TokenType.STATIC, tokenDate)
                .orElse(null);
        Assert.assertNotNull(token);
        val token2 = authStore.provisionToken(TEST_USER, TokenType.STATIC, null)
                .orElse(null);
        Assert.assertNotNull(token2);

        Assert.assertTrue(authStore.deleteExpiredTokens(testDate, sessionDuration));
        Assert.assertNull(authStore.getToken(token.getId())
                .orElse(null));
        Assert.assertNotNull(authStore.getToken(token2.getId())
                .orElse(null));
        Assert.assertTrue(authStore.deleteUser(TEST_USER));
    }*/
}
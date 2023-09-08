package com.flipkart.foxtrot.server.di;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.alerts.AlertingSystemEventConsumer;
import com.flipkart.foxtrot.core.cache.CacheFactory;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.config.OpensearchTuningConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseUtil;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.email.messageformatting.EmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.EmailSubjectBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailSubjectBuilder;
import com.flipkart.foxtrot.core.internalevents.InternalEventBus;
import com.flipkart.foxtrot.core.internalevents.InternalEventBusConsumer;
import com.flipkart.foxtrot.core.internalevents.impl.GuavaInternalEventBus;
import com.flipkart.foxtrot.core.jobs.optimization.DatabaseIndexOptimizationConfig;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.EventPublisherActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.handlers.MetricRecorder;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.handlers.SlowQueryReporter;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ClusterConfig;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchUtils;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.AuthStore;
import com.flipkart.foxtrot.server.auth.ESAuthStore;
import com.flipkart.foxtrot.server.auth.IdmanAuthStore;
import com.flipkart.foxtrot.server.auth.JwtConfig;
import com.flipkart.foxtrot.server.auth.RoleAuthorizer;
import com.flipkart.foxtrot.server.auth.TokenAuthenticator;
import com.flipkart.foxtrot.server.auth.TokenType;
import com.flipkart.foxtrot.server.auth.UserPrincipal;
import com.flipkart.foxtrot.server.auth.authprovider.AuthProvider;
import com.flipkart.foxtrot.server.auth.authprovider.ConfiguredAuthProviderFactory;
import com.flipkart.foxtrot.server.auth.sessionstore.DistributedSessionDataStore;
import com.flipkart.foxtrot.server.auth.sessionstore.SessionDataStore;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.ConsolePersistence;
import com.flipkart.foxtrot.server.console.OpensearchConsolePersistence;
import com.flipkart.foxtrot.server.jobs.consolehistory.ConsoleHistoryConfig;
import com.flipkart.foxtrot.server.jobs.sessioncleanup.SessionCleanupConfig;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreService;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreServiceImpl;
import com.foxtrot.flipkart.translator.config.SegregationConfiguration;
import com.foxtrot.flipkart.translator.config.TranslatorConfig;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.Authorizer;
import io.dropwizard.auth.CachingAuthenticator;
import io.dropwizard.auth.CachingAuthorizer;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Singleton;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.HmacKey;


/**
 *
 */
public class FoxtrotModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(TableMetadataManager.class).to(DistributedTableMetadataManager.class);
        bind(DataStore.class).to(HBaseDataStore.class);
        bind(QueryStore.class).to(OpensearchQueryStore.class);
        bind(FqlStoreService.class).to(FqlStoreServiceImpl.class);
        bind(CacheFactory.class).to(DistributedCacheFactory.class);
        bind(InternalEventBus.class).to(GuavaInternalEventBus.class);
        bind(InternalEventBusConsumer.class).to(AlertingSystemEventConsumer.class);
        bind(ConsolePersistence.class).to(OpensearchConsolePersistence.class);
        bind(EmailSubjectBuilder.class).to(StrSubstitutorEmailSubjectBuilder.class);
        bind(EmailBodyBuilder.class).to(StrSubstitutorEmailBodyBuilder.class);
        bind(TableManager.class).to(FoxtrotTableManager.class);
        bind(new TypeLiteral<List<HealthCheck>>() {
        }).toProvider(HealthcheckListProvider.class);
        bind(AuthStore.class).to(ESAuthStore.class);
        bind(SessionDataStore.class).to(DistributedSessionDataStore.class);
    }

    @Provides
    @Singleton
    public HbaseConfig hbConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getHbase();
    }

    @Provides
    @Singleton
    public OpensearchConfig esConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getOpensearchConfig();
    }

    @Provides
    @Singleton
    public TranslatorConfig getTranslatorConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getTranslatorConfig();
    }

    @Provides
    @Singleton
    public ClusterConfig clusterConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getCluster();
    }

    @Provides
    @Singleton
    public CardinalityConfig cardinalityConfig(FoxtrotServerConfiguration configuration) {
        return null == configuration.getCardinality()
               ? new CardinalityConfig("false", String.valueOf(OpensearchUtils.DEFAULT_SUB_LIST_SIZE))
               : configuration.getCardinality();
    }

    @Provides
    @Singleton
    public DatabaseIndexOptimizationConfig esIndexOptimizationConfig(FoxtrotServerConfiguration configuration) {
        return null == configuration.getEsIndexOptimizationConfig()
               ? new DatabaseIndexOptimizationConfig()
               : configuration.getEsIndexOptimizationConfig();
    }

    @Provides
    @Singleton
    public DataDeletionManagerConfig dataDeletionManagerConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getDeletionManagerConfig();
    }

    @Provides
    @Singleton
    public ConsoleHistoryConfig consoleHistoryConfig(FoxtrotServerConfiguration configuration) {
        return null == configuration.getConsoleHistoryConfig()
                ? new ConsoleHistoryConfig()
                : configuration.getConsoleHistoryConfig();
    }

    @Provides
    @Singleton
    public SessionCleanupConfig sessionCleanupConfig(FoxtrotServerConfiguration configuration) {
        return null == configuration.getSessionCleanupConfig()
                ? new SessionCleanupConfig()
                : configuration.getSessionCleanupConfig();
    }

    @Provides
    @Singleton
    public CacheConfig cacheConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getCacheConfig();
    }

    @Provides
    @Singleton
    public EmailConfig emailConfig(FoxtrotServerConfiguration configuration) {
        return configuration.getEmailConfig();
    }

    @Provides
    @Singleton
    public SegregationConfiguration segregationConfiguration(FoxtrotServerConfiguration configuration) {
        return configuration.getSegregationConfiguration();
    }

    @Provides
    @Singleton
    public ObjectMapper objectMapper(Environment environment) {
        return environment.getObjectMapper();
    }

    @Provides
    @Singleton
    public List<IndexerEventMutator> provideMutators(
            FoxtrotServerConfiguration configuration,
            ObjectMapper objectMapper) {
        return Collections.singletonList(new LargeTextNodeRemover(objectMapper, configuration.getTextNodeRemover()));
    }

    @Provides
    @Singleton
    public List<ActionExecutionObserver> actionExecutionObservers(
            CacheManager cacheManager,
            InternalEventBus eventBus) {
        return ImmutableList.<ActionExecutionObserver>builder()
                .add(new MetricRecorder())
                .add(new ResponseCacheUpdater(cacheManager))
                .add(new SlowQueryReporter())
                .add(new EventPublisherActionExecutionObserver(eventBus))
                .build();
    }

    @Provides
    @Singleton
    public ExecutorService provideGlobalExecutorService(Environment environment) {
        return environment.lifecycle()
                .executorService("query-executor-%s")
                .minThreads(20)
                .maxThreads(30)
                .keepAliveTime(Duration.seconds(30))
                .build();
    }

    @Provides
    @Singleton
    public ScheduledExecutorService provideGlobalScheduledExecutorService(Environment environment) {
        return environment.lifecycle()
                .scheduledExecutorService("cardinality-executor")
                .threads(1)
                .build();
    }

    @Provides
    @Singleton
    public Configuration provideHBaseConfiguration(HbaseConfig hbaseConfig) throws IOException {
        return HBaseUtil.create(hbaseConfig);
    }

    @Provides
    @Singleton
    public ServerFactory serverFactory(FoxtrotServerConfiguration configuration) {
        return configuration.getServerFactory();
    }

    @Provides
    @Singleton
    public AuthConfig authConfig(FoxtrotServerConfiguration serverConfiguration) {
        return serverConfiguration.getAuth();
    }

/*    @Provides
    @Singleton
    public GoogleAuthProviderConfig googleAuthProviderConfig(FoxtrotServerConfiguration configuration) {
        return (GoogleAuthProviderConfig)configuration.getAuth().getProvider();
    }*/

    @Provides
    @Singleton
    public AuthProvider authProvider(
            FoxtrotServerConfiguration configuration,
            AuthConfig authConfig,
            Environment environment,
            Injector injector) {
        val authType = authConfig.getProvider().getType();
        AuthStore authStore = null;
        switch (authType) {
            case NONE: {
                break;
            }
            case OAUTH_GOOGLE:
                authStore = injector.getInstance(ESAuthStore.class);
                break;
            case OAUTH_IDMAN:
                authStore = injector.getInstance(IdmanAuthStore.class);
                break;
            default: {
                throw new IllegalArgumentException("Mode " + authType.name() + " not supported");
            }
        }
        return new ConfiguredAuthProviderFactory(configuration.getAuth())
                .build(environment.getObjectMapper(), authStore);
    }

    @Provides
    @Singleton
    public JwtConsumer provideJwtConsumer(AuthConfig config) {
        final JwtConfig jwtConfig = config.getJwt();
        final byte[] secretKey = jwtConfig.getPrivateKey().getBytes(StandardCharsets.UTF_8);
        return new JwtConsumerBuilder()
                .setRequireIssuedAt()
                .setRequireSubject()
                .setExpectedIssuer(jwtConfig.getIssuerId())
                .setVerificationKey(new HmacKey(secretKey))
                .setJwsAlgorithmConstraints(new AlgorithmConstraints(
                        AlgorithmConstraints.ConstraintType.WHITELIST,
                        AlgorithmIdentifiers.HMAC_SHA512))
                .setExpectedAudience(Arrays.stream(TokenType.values())
                        .map(TokenType::name)
                        .toArray(String[]::new))
                .build();
    }

    @Provides
    @Singleton
    public Authenticator<String, UserPrincipal> authenticator(
            final Environment environment,
            final TokenAuthenticator authenticator,
            final AuthConfig authConfig) {
        return new CachingAuthenticator<>(
                environment.metrics(),
                authenticator,
                CacheBuilderSpec.parse(authConfig.getJwt().getAuthCachePolicy()));
    }

    @Provides
    @Singleton
    public Authorizer<UserPrincipal> authorizer(
            final Environment environment,
            final RoleAuthorizer authorizer,
            final AuthConfig authConfig) {
        return new CachingAuthorizer<>(environment.metrics(),
                authorizer,
                CacheBuilderSpec.parse(authConfig.getJwt().getAuthCachePolicy()));
    }

    @Provides
    @Singleton
    public OpensearchTuningConfig provideOpensearchTuningConfig(FoxtrotServerConfiguration configuration) {
        return Objects.nonNull(configuration.getOpensearchTuningConfig())
               ? configuration.getOpensearchTuningConfig()
               : new OpensearchTuningConfig();
    }

}

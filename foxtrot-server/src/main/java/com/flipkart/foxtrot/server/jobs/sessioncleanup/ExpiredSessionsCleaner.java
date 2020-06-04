package com.flipkart.foxtrot.server.jobs.sessioncleanup;

import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.AuthStore;
import com.flipkart.foxtrot.server.utils.AuthUtils;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

/**
 *
 */
@Singleton
@Order(55)
@Slf4j
public class ExpiredSessionsCleaner extends BaseJobManager {

    private final SessionCleanupConfig sessionCleanupConfig;
    private final Provider<AuthStore> authStore;
    private final AuthConfig authConfig;

    @Inject
    public ExpiredSessionsCleaner(SessionCleanupConfig sessionCleanupConfig,
                                  ScheduledExecutorService scheduledExecutorService,
                                  HazelcastConnection hazelcastConnection,
                                  Provider<AuthStore> authStore,
                                  AuthConfig authConfig) {
        super(sessionCleanupConfig, scheduledExecutorService, hazelcastConnection);
        this.sessionCleanupConfig = sessionCleanupConfig;
        this.authStore = authStore;
        this.authConfig = authConfig;
    }

    @Override
    protected void runImpl(LockingTaskExecutor executor,
                           Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                authStore.get()
                        .deleteExpiredTokens(new Date(), AuthUtils.sessionDuration(authConfig));
            } catch (Exception e) {
                log.error("Session cleanup failed: ", e);
            }
        }, new LockConfiguration(sessionCleanupConfig.getJobName(), lockAtMostUntil));
    }
}

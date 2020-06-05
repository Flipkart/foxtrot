package com.flipkart.foxtrot.core.lock;

import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import net.javacrumbs.shedlock.provider.hazelcast.HazelcastLockProvider;

@Slf4j
@Singleton
public class LockedExecutor {

    private static final DistributedLockGroupConfig defaultDistributedLockGroupConfig;

    static {
        defaultDistributedLockGroupConfig = new DistributedLockGroupConfig();
        defaultDistributedLockGroupConfig.setLockExpiryTimeInMs(2000);
    }

    /**
     * distributed lock provider
     */
    private final LockProvider lockProvider;
    private final HazelcastDistributedLockConfig distributedLockConfig;

    @Inject
    public LockedExecutor(final HazelcastDistributedLockConfig distributedLockConfig,
                          final HazelcastConnection hazelcastConnection) {
        this.lockProvider = new HazelcastLockProvider(hazelcastConnection.getHazelcast());
        this.distributedLockConfig = distributedLockConfig;
    }

    /**
     * Do work in single lock
     */
    public <T, R> R doItInLock(T dataForProcessing,
                               Function<T, R> runFuncInsideLock,
                               Function<T, R> runFuncIfLockNotAcquired,
                               Function<T, String> uniqueLockIdFunc,
                               String lockGroupName,
                               R defaultDataWhenLockNotAcquiredOrNoFailFunction) {
        String lockString = uniqueLockIdFunc.apply(dataForProcessing);

        Instant lockAtMostUntil = getLockAtMostUntil(lockGroupName);
        LockConfiguration lockConfig = new LockConfiguration(lockString, lockAtMostUntil);

        Optional<SimpleLock> lock = this.lockProvider.lock(lockConfig);

        if (lock.isPresent()) {
            try {
                log.debug("Locked {}.", lockConfig.getName());
                return runFuncInsideLock.apply(dataForProcessing);
            } finally {
                ((SimpleLock) lock.get()).unlock();
                log.debug("Unlocked {}.", lockConfig.getName());
            }
        } else {
            log.debug("Not executing {}. It's locked.", lockConfig.getName());
            if (runFuncIfLockNotAcquired != null) {
                log.info("Lock was not taken for work, run default handler: lockId={}, lockGroupName={}", lockString,
                        lockGroupName);
                return runFuncIfLockNotAcquired.apply(dataForProcessing);
            }
            log.info("Lock was not taken for work, return default data: lockId={}, lockGroupName={}", lockString,
                    lockGroupName);
            return defaultDataWhenLockNotAcquiredOrNoFailFunction;
        }
    }

    private Instant getLockAtMostUntil(String lockGroupName) {
        DistributedLockGroupConfig lockGroupConfig;
        if (distributedLockConfig == null || distributedLockConfig.getLocksConfig() == null
                || !distributedLockConfig.getLocksConfig()
                .containsKey(lockGroupName)) {
            lockGroupConfig = defaultDistributedLockGroupConfig;
        } else {
            lockGroupConfig = distributedLockConfig.getLocksConfig()
                    .get(lockGroupName);
        }

        return Instant.now()
                .plusSeconds(TimeUnit.MINUTES.toSeconds(lockGroupConfig.getLockExpiryTimeInMs()));
    }

    // Method for convenience
    public <T, R> R doItInLockV1(T dataForProcessing,
                                 Function<T, R> runFuncInsideLock,
                                 Function<T, R> runFuncIfLockNotAcquired,
                                 String lockString,
                                 String lockGroupName) {
        return doItInLock(dataForProcessing, runFuncInsideLock, runFuncIfLockNotAcquired, t -> lockString,
                lockGroupName, null);
    }

    // Method for convenience
    public <T, R> R doItInLockV2(T dataForProcessing,
                                 Function<T, R> runFuncInsideLock,
                                 Function<T, String> uniqueLockIdFunc,
                                 String lockGroupName,
                                 R defaultDataWhenLockNotAcquiredOrNoFailFunction) {
        return doItInLock(dataForProcessing, runFuncInsideLock, null, uniqueLockIdFunc, lockGroupName,
                defaultDataWhenLockNotAcquiredOrNoFailFunction);
    }

    // Method for convenience
    public <T, R> R doItInLockV3(T dataForProcessing,
                                 Function<T, R> runFuncInsideLock,
                                 String lockString,
                                 String lockGroupName,
                                 R defaultDataWhenLockNotAcquiredOrNoFailFunction) {
        return doItInLock(dataForProcessing, runFuncInsideLock, null, t -> lockString, lockGroupName,
                defaultDataWhenLockNotAcquiredOrNoFailFunction);
    }

    // Method for convenience
    public <T, R> R doItInLockV4(T dataForProcessing,
                                 Function<T, R> runFuncInsideLock,
                                 Function<T, String> uniqueLockIdFunc,
                                 String lockGroupName) {
        return doItInLock(dataForProcessing, runFuncInsideLock, null, uniqueLockIdFunc, lockGroupName, null);
    }

    // Method for convenience
    public <T, R> R doItInLockV5(T dataForProcessing,
                                 Function<T, R> runFuncInsideLock,
                                 String lockString,
                                 String lockGroupName) {
        return doItInLock(dataForProcessing, runFuncInsideLock, null, t -> lockString, lockGroupName, null);
    }

    // Method for convenience
    public <T, R> R doItInLockV6(T dataForProcessing,
                                 Function<T, R> runFuncInsideLock,
                                 Function<T, R> runFuncIfLockNotAcquired,
                                 String lockString) {
        return doItInLock(dataForProcessing, runFuncInsideLock, runFuncIfLockNotAcquired, t -> lockString, "default",
                null);
    }
}
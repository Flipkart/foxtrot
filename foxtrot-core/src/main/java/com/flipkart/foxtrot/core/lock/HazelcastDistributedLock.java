package com.flipkart.foxtrot.core.lock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.flipkart.foxtrot.common.exception.FunnelException;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.google.common.base.Strings;
import com.hazelcast.core.ILock;
import java.util.concurrent.locks.Lock;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
@AllArgsConstructor
public class HazelcastDistributedLock implements DistributedLock {

    private static final DistributedLockGroupConfig defaultDistributedLockGroupConfig;

    static {
        defaultDistributedLockGroupConfig = new DistributedLockGroupConfig();
        defaultDistributedLockGroupConfig.setLockExpiryTimeInMs(2000);
    }

    private final HazelcastConnection hazelcastConnection;
    private final HazelcastDistributedLockConfig lockConfig;

    @Inject
    public HazelcastDistributedLock(final HazelcastDistributedLockConfig distributedLockConfig,
            final HazelcastConnection hazelcastConnection) {
        this.hazelcastConnection = hazelcastConnection;
        this.lockConfig = distributedLockConfig;
    }

    @Override
    public Lock achieveLock(String lockUniqueId, String lockGroup) {
        if (Strings.isNullOrEmpty(lockUniqueId)) {
            log.error("Failed to take HZ lock as lock id is null or empty: lockUniqueId={}", lockUniqueId);
            throw new FunnelException("Failed to take HZ lock as lock id is null or empty");
        }

        DistributedLockGroupConfig lockGroupConfig;
        if (lockConfig == null || lockConfig.getLocksConfig() == null || !lockConfig.getLocksConfig()
                .containsKey(lockGroup)) {
            lockGroupConfig = defaultDistributedLockGroupConfig;
        } else {
            lockGroupConfig = lockConfig.getLocksConfig()
                    .get(lockGroup);
        }

        ILock lock = hazelcastConnection.getHazelcast()
                .getLock(lockUniqueId);
        try {
            log.debug("(HazelcastDistributedLock:group={}) try lock={}, timeout={}", lockGroup, lockUniqueId,
                    lockGroupConfig.getLockExpiryTimeInMs());
            if (!lock.tryLock(lockGroupConfig.getLockExpiryTimeInMs(), MILLISECONDS)) {
                log.error("Failed to acquired HZ lock with lock group={}, id={}, timeout={}", lockGroup, lockUniqueId,
                        lockGroupConfig.getLockExpiryTimeInMs());
                throw new DistributedLockTryFailedWithTimeout(
                        "Failed to take HZ lock with lock id = " + lockUniqueId + " group=" + lockGroup + " timeout="
                                + lockGroupConfig.getLockExpiryTimeInMs());
            }
            log.debug("(HazelcastDistributedLock:group={}) lock acquired={}, timeout={}", lockGroup, lockUniqueId,
                    lockGroupConfig.getLockExpiryTimeInMs());
        } catch (InterruptedException e) {
            log.error("Failed to acquired HZ lock with lock id = {}, error={}", lockUniqueId, e.getMessage());
            Thread.currentThread()
                    .interrupt();
            throw new FunnelException("HZ lock interrupted with lock id = " + lockUniqueId);
        }
        return lock;
    }

    @Override
    public Lock achieveLock(String lockUniqueId) {
        if (Strings.isNullOrEmpty(lockUniqueId)) {
            log.error("Failed to take HZ lock as lock id is null or empty: lockUniqueId={}", lockUniqueId);
            throw new FunnelException("Failed to take HZ lock as lock id is null or empty");
        }

        ILock lock = hazelcastConnection.getHazelcast()
                .getLock(lockUniqueId);
        try {
            log.debug("(HazelcastDistributedLock) try lock={}", lockUniqueId);
            if (!lock.tryLock(lock.getRemainingLeaseTime(), MILLISECONDS)) {
                log.error("Failed to acquired HZ lock with lock id = {}", lockUniqueId);
                throw new FunnelException("Failed to take HZ lock with lock id = " + lockUniqueId);
            }
            log.debug("(HazelcastDistributedLock) lock acquired={}", lockUniqueId);
        } catch (InterruptedException e) {
            log.error("Failed to acquired HZ lock with lock id = {}, error={}", lockUniqueId, e.getMessage());
            Thread.currentThread()
                    .interrupt();
            throw new FunnelException("HZ lock interrupted with lock id = " + lockUniqueId);
        }
        return lock;
    }

    @Override
    public void releaseLock(Lock lock, String lockUniqueId) {
        try {
            log.debug("(HazelcastDistributedLock) release lock={}", lockUniqueId);
            lock.unlock();
        } catch (Exception e) {
            log.debug("(HazelcastDistributedLock) failed to release lock={}", lockUniqueId);
        }
    }

    @Override
    public void start() {
        //To implement
    }

    @Override
    public void shutdown() {
        //To implement
    }

}

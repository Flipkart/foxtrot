package com.flipkart.foxtrot.core.lock;

import com.flipkart.foxtrot.core.lock.DistributedLock.DistributedLockTryFailedWithTimeout;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class LockedExecutor {

    /**
     * distributed lock object
     */
    private final DistributedLock distributedLock;

    @Inject
    public LockedExecutor(final DistributedLock distributedLock) {
        this.distributedLock = distributedLock;
    }

    /**
     * Do work in single lock
     */
    public <T, R> R doItInLock(T dataForProcessing, Function<T, R> runFuncInsideLock,
            Function<T, String> uniqueLockIdFunc, R defaultDataWhenLockNotAcquired) {
        String lockString = uniqueLockIdFunc.apply(dataForProcessing);
        Lock lock = null;
        try {
            lock = distributedLock.achieveLock(lockString);
            if (lock != null) {
                return runFuncInsideLock.apply(dataForProcessing);
            }
            log.info("Lock was not taken for work, return default data: lockId={}, defaultData={}", lockString,
                    defaultDataWhenLockNotAcquired);
            return defaultDataWhenLockNotAcquired;
        } finally {
            if (lock != null) {
                distributedLock.releaseLock(lock, lockString);
            }
        }
    }

    public <T, R> R doItInLock(T dataForProcessing, Function<T, R> runFuncInsideLock,
            Function<T, R> runFuncIfLockNotAcquired, Function<T, String> uniqueLockIdFunc, String lockGroupName,
            R defaultDataWhenLockNotAcquiredOrNoFailFunction) {
        String lockString = uniqueLockIdFunc.apply(dataForProcessing);
        Lock lock = null;
        try {
            try {
                lock = distributedLock.achieveLock(lockString, lockGroupName);
                if (lock != null) {
                    return runFuncInsideLock.apply(dataForProcessing);
                }
            } catch (DistributedLockTryFailedWithTimeout e) {
                if (runFuncIfLockNotAcquired != null) {
                    log.info("Lock was not taken for work, run default handler: lockId={}, lockGroupName={}",
                            lockString, lockGroupName);
                    return runFuncIfLockNotAcquired.apply(dataForProcessing);
                }
            }
            return defaultDataWhenLockNotAcquiredOrNoFailFunction;
        } finally {
            if (lock != null) {
                distributedLock.releaseLock(lock, lockString);
            }
        }
    }

    // Method for convenience
    public <T, R> R doItInLockV1(T dataForProcessing, Function<T, R> runFuncInsideLock,
            Function<T, R> runFuncIfLockNotAcquired, String lockString, String lockGroupName) {
        return doItInLock(dataForProcessing, runFuncInsideLock, runFuncIfLockNotAcquired, t -> lockString,
                lockGroupName, null);
    }

    // Method for convenience
    public <T, R> R doItInLockV2(T dataForProcessing, Function<T, R> runFuncInsideLock,
            Function<T, String> uniqueLockIdFunc, String lockGroupName,
            R defaultDataWhenLockNotAcquiredOrNoFailFunction) {
        return doItInLock(dataForProcessing, runFuncInsideLock, null, uniqueLockIdFunc, lockGroupName,
                defaultDataWhenLockNotAcquiredOrNoFailFunction);
    }

    // Method for convenience
    public <T, R> R doItInLockV3(T dataForProcessing, Function<T, R> runFuncInsideLock, String lockString,
            String lockGroupName, R defaultDataWhenLockNotAcquiredOrNoFailFunction) {
        return doItInLock(dataForProcessing, runFuncInsideLock, null, t -> lockString, lockGroupName,
                defaultDataWhenLockNotAcquiredOrNoFailFunction);
    }

    // Method for convenience
    public <T, R> R doItInLockV4(T dataForProcessing, Function<T, R> runFuncInsideLock,
            Function<T, String> uniqueLockIdFunc, String lockGroupName) {
        return doItInLock(dataForProcessing, runFuncInsideLock, null, uniqueLockIdFunc, lockGroupName, null);
    }

    // Method for convenience
    public <T, R> R doItInLockV5(T dataForProcessing, Function<T, R> runFuncInsideLock, String lockString,
            String lockGroupName) {
        return doItInLock(dataForProcessing, runFuncInsideLock, null, t -> lockString, lockGroupName, null);
    }

    // Method for convenience
    public <T, R> R doItInLockV6(T dataForProcessing, Function<T, R> runFuncInsideLock,
            Function<T, R> runFuncIfLockNotAcquired, String lockString) {
        return doItInLock(dataForProcessing, runFuncInsideLock, runFuncIfLockNotAcquired, t -> lockString, "default",
                null);
    }
}
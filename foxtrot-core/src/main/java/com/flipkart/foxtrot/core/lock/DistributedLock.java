package com.flipkart.foxtrot.core.lock;

import java.util.concurrent.locks.Lock;

public interface DistributedLock {

    default Lock achieveLock(String lockUniqueId, String lockGroup) {
        return achieveLock(lockUniqueId);
    }

    Lock achieveLock(String lockUniqueId);

    void releaseLock(Lock lock, String lockUniqueId);

    void start();

    void shutdown();

    public static class DistributedLockTryFailedWithTimeout extends RuntimeException {

        DistributedLockTryFailedWithTimeout(String message) {
            super(message);
        }
    }
}

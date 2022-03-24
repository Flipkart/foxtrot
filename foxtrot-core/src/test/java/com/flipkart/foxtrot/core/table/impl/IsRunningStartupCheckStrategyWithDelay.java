package com.flipkart.foxtrot.core.table.impl;

import com.github.dockerjava.api.DockerClient;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@Slf4j
public class IsRunningStartupCheckStrategyWithDelay extends IsRunningStartupCheckStrategy {

    @Override
    public StartupStatus checkStartupState(DockerClient dockerClient, String containerId) {

        try {
            await().pollDelay(5000, TimeUnit.MILLISECONDS).until(() -> true);
        } catch (Exception e) {
            log.error("Unable to pause thread", e);
        }

        return super.checkStartupState(dockerClient, containerId);
    }
}

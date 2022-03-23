package com.flipkart.foxtrot.server.di;

import com.codahale.metrics.health.HealthCheck;
import com.flipkart.foxtrot.server.healthcheck.HBaseHealthCheck;
import com.flipkart.foxtrot.server.healthcheck.HazelcastHealthCheck;
import com.google.inject.Inject;
import com.google.inject.Provider;

import java.util.Arrays;
import java.util.List;

public class HealthcheckListProvider implements Provider<List<HealthCheck>> {

    @Inject
    private Provider<HBaseHealthCheck> hBaseHealthCheckProvider;

    @Inject
    private Provider<HazelcastHealthCheck> hazelcastHealthCheckProvider;


    public List<HealthCheck> get() {
        return Arrays.asList(hBaseHealthCheckProvider.get(), hazelcastHealthCheckProvider.get());
    }
}
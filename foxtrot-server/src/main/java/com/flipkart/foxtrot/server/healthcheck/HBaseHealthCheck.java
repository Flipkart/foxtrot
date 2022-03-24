package com.flipkart.foxtrot.server.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import ru.vyarus.dropwizard.guice.module.installer.feature.health.NamedHealthCheck;

@Singleton
public class HBaseHealthCheck extends NamedHealthCheck {

    private static final String HBASE_HEALTHCHECK = "hbaseHealthcheck";
    private Configuration configuration;

    @Inject
    public HBaseHealthCheck(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected Result check() throws Exception {
        try {
            HBaseAdmin.checkHBaseAvailable(configuration);
            return HealthCheck.Result.
                    healthy("HBase running");
        } catch (Exception e) {
            return HealthCheck.Result.unhealthy(e);
        }
    }

    @Override
    public String getName() {
        return HBASE_HEALTHCHECK;
    }
}

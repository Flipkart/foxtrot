package com.flipkart.foxtrot.server.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import ru.vyarus.dropwizard.guice.module.installer.feature.health.NamedHealthCheck;

@Singleton
public class HBaseHealthCheck extends NamedHealthCheck {

    private Configuration configuration;

    private static final String HBASE_HEALTHCHECK = "hbaseHealthcheck";

    @Inject
    public HBaseHealthCheck(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected Result check() throws Exception {
        try {
            HBaseAdmin.checkHBaseAvailable(configuration);

            return HealthCheck.Result.builder()
                    .healthy()
                    .withMessage("HBase running")
                    .build();
        } catch (Exception e) {
            return HealthCheck.Result.builder()
                    .unhealthy(e)
                    .build();
        }
    }

    @Override
    public String getName() {
        return HBASE_HEALTHCHECK;
    }
}

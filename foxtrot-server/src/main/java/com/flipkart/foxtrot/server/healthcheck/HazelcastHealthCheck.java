package com.flipkart.foxtrot.server.healthcheck;

import static com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection.HEALTHCHECK_MAP;

import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.UUID;

import ru.vyarus.dropwizard.guice.module.installer.feature.health.NamedHealthCheck;

@Singleton
public class HazelcastHealthCheck extends NamedHealthCheck {

    private static final String HAZELCAST_HEALTHCHECK = "hazelcastHealthcheck";
    private HazelcastConnection hazelcastConnection;
    /**
     * A random UUID to healthcheck
     */
    private String uuid = UUID.randomUUID().toString();
    /**
     * A counter for healthcheck
     */
    private int counter = 0;

    @Inject
    public HazelcastHealthCheck(final HazelcastConnection hazelcastConnection) {
        this.hazelcastConnection = hazelcastConnection;
    }

    @Override
    protected Result check() throws Exception {
        // Update the counter and store in the map
        counter = counter + 1;
        try {
            hazelcastConnection.getHazelcast().getMap(HEALTHCHECK_MAP).put(uuid, counter);
            int toCheck = (int) hazelcastConnection.getHazelcast().getMap(HEALTHCHECK_MAP).get(uuid);
            return toCheck == counter ? Result.healthy("UUID:" + uuid + ", counter: " + counter + " - OK")
                    : Result.unhealthy("UUID:" + uuid + ", counter: " + counter
                    + " Something is wrong: healthCheck count is not updating");
        } catch (Exception e) {
            return Result.healthy("UUID:" + uuid + " - Error: " + e.getMessage());
        }
    }

    @Override
    public String getName() {
        return HAZELCAST_HEALTHCHECK;
    }
}

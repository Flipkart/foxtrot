package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.yammer.metrics.core.HealthCheck;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;

/**
 * Created by rishabh.goyal on 15/05/14.
 */

public class ElasticSearchHealthCheck extends HealthCheck{
    private ElasticsearchConnection elasticsearchConnection;

    public ElasticSearchHealthCheck(String name, ElasticsearchConnection elasticsearchConnection) {
        super(name);
        this.elasticsearchConnection = elasticsearchConnection;
    }

    @Override
    protected HealthCheck.Result check() throws Exception {
        ClusterHealthResponse response = elasticsearchConnection
                .getClient()
                .admin()
                .cluster()
                .prepareHealth()
                .execute()
                .actionGet();
        return (response.getStatus().name().equalsIgnoreCase("GREEN")
                || response.getStatus().name().equalsIgnoreCase("YELLOW"))
                ? HealthCheck.Result.healthy(): HealthCheck.Result.unhealthy("Cluster unhealthy");
    }
}

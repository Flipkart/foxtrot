/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.server.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;

/**
 * Created by rishabh.goyal on 15/05/14.
 */

public class ElasticSearchHealthCheck extends HealthCheck {

    private ElasticsearchConnection elasticsearchConnection;

    public ElasticSearchHealthCheck(ElasticsearchConnection elasticsearchConnection) {
        this.elasticsearchConnection = elasticsearchConnection;
    }

    @Override
    protected HealthCheck.Result check() throws Exception {
        ClusterHealthResponse response = elasticsearchConnection.getClient()
                .cluster()
                .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        return (response.getStatus()
                .name()
                .equalsIgnoreCase("GREEN") || response.getStatus()
                .name()
                .equalsIgnoreCase("YELLOW")) ? HealthCheck.Result.healthy() : HealthCheck.Result.unhealthy("Cluster unhealthy");
    }
}

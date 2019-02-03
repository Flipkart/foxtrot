/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationConfig;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ClusterConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import io.dropwizard.Configuration;
import io.dropwizard.discovery.bundle.ServiceDiscoveryConfiguration;
import io.dropwizard.riemann.RiemannConfig;
import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:26 PM
 */
@Data
public class FoxtrotServerConfiguration extends Configuration {
    @Valid
    private final HbaseConfig hbase;

    @Valid
    private final ElasticsearchConfig elasticsearch;

    @Valid
    private final ClusterConfig cluster;

    @NotNull
    @Valid
    private ServiceDiscoveryConfiguration serviceDiscovery;

    @NotNull
    @Valid
    private RiemannConfig riemann;

    @Valid
    private CardinalityConfig cardinality;

    @Valid
    private EsIndexOptimizationConfig esIndexOptimizationConfig;

    @Valid
    @JsonProperty("deletionconfig")
    private final DataDeletionManagerConfig deletionManagerConfig;

    private CacheConfig cacheConfig;

    public FoxtrotServerConfiguration() {
        this.hbase = new HbaseConfig();
        this.elasticsearch = new ElasticsearchConfig();
        this.cluster = new ClusterConfig();
        this.deletionManagerConfig = new DataDeletionManagerConfig();
    }

    public HbaseConfig getHbase() {
        return hbase;
    }

    public ElasticsearchConfig getElasticsearch() {
        return elasticsearch;
    }

    public ClusterConfig getCluster() {
        return cluster;
    }

    public DataDeletionManagerConfig getTableDataManagerConfig() {
        return deletionManagerConfig;
    }

    public ServiceDiscoveryConfiguration getServiceDiscovery() {
        return serviceDiscovery;
    }

    public void setServiceDiscovery(ServiceDiscoveryConfiguration serviceDiscovery) {
        this.serviceDiscovery = serviceDiscovery;
    }

    public RiemannConfig getRiemann() {
        return riemann;
    }

    public void setRiemann(RiemannConfig riemann) {
        this.riemann = riemann;
    }
}

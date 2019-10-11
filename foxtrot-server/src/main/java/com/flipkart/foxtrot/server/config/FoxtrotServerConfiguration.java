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
package com.flipkart.foxtrot.server.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.foxtrot.core.alerts.EmailConfig;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationConfig;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ClusterConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.reroute.ClusterRerouteConfig;
import com.flipkart.foxtrot.server.jobs.consolehistory.ConsoleHistoryConfig;
import com.phonepe.gandalf.models.client.GandalfClientConfig;
import io.appform.dropwizard.discovery.bundle.ServiceDiscoveryConfiguration;
import io.dropwizard.Configuration;
import io.dropwizard.primer.model.PrimerBundleConfiguration;
import io.dropwizard.riemann.RiemannConfig;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com) Date: 15/03/14 Time: 9:26 PM
 */
@Data
public class FoxtrotServerConfiguration extends Configuration {

    @Valid
    private final HbaseConfig hbase;

    @Valid
    private final ElasticsearchConfig elasticsearch;

    @Valid
    private final ClusterConfig cluster;
    @Valid
    @JsonProperty("deletionconfig")
    private final DataDeletionManagerConfig deletionManagerConfig;

    private SwaggerBundleConfiguration swagger;

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
    private ConsoleHistoryConfig consoleHistoryConfig;
    private EmailConfig emailConfig;
    private CacheConfig cacheConfig;

    private QueryConfig queryConfig;

    @NonNull
    private GandalfClientConfig gandalfConfig = new GandalfClientConfig();

    @NonNull
    private PrimerBundleConfiguration primerBundleConfiguration;

    private RangerConfiguration rangerConfiguration;

    private SegregationConfiguration segregationConfiguration;

    @NotNull
    private boolean restrictAccess;

    private GandalfConfiguration gandalfConfiguration;

    @Valid
    @Builder.Default
    private TextNodeRemoverConfiguration textNodeRemover = new TextNodeRemoverConfiguration();

    private ClusterRerouteConfig clusterRerouteConfig;

    private ElasticsearchTuningConfig elasticsearchTuningConfig;

    public FoxtrotServerConfiguration() {
        this.hbase = new HbaseConfig();
        this.elasticsearch = new ElasticsearchConfig();
        this.cluster = new ClusterConfig();
        this.deletionManagerConfig = new DataDeletionManagerConfig();
        this.emailConfig = new EmailConfig();
        this.segregationConfiguration = new SegregationConfiguration();
        this.serviceDiscovery = new ServiceDiscoveryConfiguration();
        this.riemann = new RiemannConfig();
        this.restrictAccess = true;
        this.queryConfig = new QueryConfig();
        this.clusterRerouteConfig = new ClusterRerouteConfig();
        this.elasticsearchTuningConfig = new ElasticsearchTuningConfig();
    }
}

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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.foxtrot.core.cardinality.CardinalityConfig;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.config.*;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationConfig;
import com.flipkart.foxtrot.core.lock.HazelcastDistributedLockConfig;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ClusterConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.pipeline.PipelineConfiguration;
import com.flipkart.foxtrot.pipeline.resources.GeojsonStoreConfiguration;
import com.foxtrot.flipkart.translator.config.SegregationConfiguration;
import com.foxtrot.flipkart.translator.config.TranslatorConfig;
import io.appform.dropwizard.discovery.bundle.ServiceDiscoveryConfiguration;
import io.dropwizard.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:26 PM
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class FoxtrotServerConfiguration extends Configuration {

    @Valid
    private final HbaseConfig hbase;

    @Valid
    @JsonIgnoreProperties(ignoreUnknown = true)
    private final ElasticsearchConfig elasticsearch;

    @Valid
    private final ClusterConfig cluster;

    @Valid
    @JsonProperty("deletionconfig")
    private final DataDeletionManagerConfig deletionManagerConfig;

    @Valid
    private final ShardRebalanceJobConfig shardRebalanceJobConfig;

    private SwaggerBundleConfiguration swagger;

    @NotNull
    @Valid
    private ServiceDiscoveryConfiguration serviceDiscovery;

    @Valid
    private CardinalityConfig cardinality;

    @Valid
    private EsIndexOptimizationConfig esIndexOptimizationConfig;

    @Valid
    private TableIndexMetadataJobConfig tableIndexMetadataJobConfig;

    @Valid
    private IndexMetadataCleanupJobConfig indexMetadataCleanupJobConfig;

    @Valid
    private ShardCountTuningJobConfig shardCountTuningJobConfig;

    @Valid
    private ConsoleHistoryConfig consoleHistoryConfig;

    @Valid
    private EmailConfig emailConfig;

    @Valid
    private CacheConfig cacheConfig;

    private QueryConfig queryConfig;

    @Valid
    private RangerConfiguration rangerConfiguration;

    @Valid
    private SegregationConfiguration segregationConfiguration;

    @NotNull
    private boolean restrictAccess;

    private List<String> restrictedTables;

    private boolean authenticationEnabled = true;

    private FunnelConfiguration funnelConfiguration;

    private HazelcastDistributedLockConfig distributedLockConfig;

    @Valid
    private ElasticsearchTuningConfig elasticsearchTuningConfig;

    @Valid
    private String swaggerHost;

    private String swaggerScheme;

    @Builder.Default
    private TranslatorConfig translatorConfig = new TranslatorConfig();

    @Valid
    @Builder.Default
    private TextNodeRemoverConfiguration textNodeRemover = new TextNodeRemoverConfiguration();

    @Valid
    private NodeGroupActivityConfig nodeGroupActivityConfig;

    @Valid
    private PipelineConfiguration pipelineConfiguration;

    @Valid
    private GeojsonStoreConfiguration geojsonStoreConfiguration;

    public FoxtrotServerConfiguration() {
        this.hbase = new HbaseConfig();
        this.elasticsearch = new ElasticsearchConfig();
        this.cluster = new ClusterConfig();
        this.deletionManagerConfig = new DataDeletionManagerConfig();
        this.emailConfig = new EmailConfig();
        this.segregationConfiguration = new SegregationConfiguration();
        this.serviceDiscovery = new ServiceDiscoveryConfiguration();
        this.restrictAccess = true;
        this.restrictedTables = new ArrayList<>();
        this.queryConfig = new QueryConfig();
        this.elasticsearchTuningConfig = new ElasticsearchTuningConfig();
        this.pipelineConfiguration = new PipelineConfiguration();
        this.shardRebalanceJobConfig = new ShardRebalanceJobConfig();
        this.geojsonStoreConfiguration = new GeojsonStoreConfiguration();
        this.shardCountTuningJobConfig = new ShardCountTuningJobConfig();
        this.tableIndexMetadataJobConfig = new TableIndexMetadataJobConfig();
    }
}

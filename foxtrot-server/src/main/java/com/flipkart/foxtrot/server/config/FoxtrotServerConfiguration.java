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
import com.flipkart.foxtrot.core.config.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.config.TextNodeRemoverConfiguration;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.jobs.optimization.EsIndexOptimizationConfig;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ClusterConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.core.reroute.ClusterRerouteConfig;
import com.flipkart.foxtrot.server.jobs.consolehistory.ConsoleHistoryConfig;
import com.flipkart.foxtrot.server.jobs.sessioncleanup.SessionCleanupConfig;
import com.foxtrot.flipkart.translator.config.SegregationConfiguration;
import com.foxtrot.flipkart.translator.config.TranslatorConfig;
import io.dropwizard.Configuration;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

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
    private final ElasticsearchConfig elasticsearch;

    @Valid
    private final ClusterConfig cluster;
    @Valid
    @JsonProperty("deletionconfig")
    private final DataDeletionManagerConfig deletionManagerConfig;

    @Valid
    private CardinalityConfig cardinality;
    @Valid
    private EsIndexOptimizationConfig esIndexOptimizationConfig;

    @Valid
    private SessionCleanupConfig sessionCleanupConfig;

    @Valid
    private ConsoleHistoryConfig consoleHistoryConfig;
    @Valid
    private EmailConfig emailConfig;
    @Valid
    private CacheConfig cacheConfig;

    @Valid
    private RangerConfiguration rangerConfiguration;

    @Valid
    private SegregationConfiguration segregationConfiguration;

    @NotNull
    private boolean restrictAccess;

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

    private ClusterRerouteConfig clusterRerouteConfig;

    @Valid
    @NotNull
    private AuthConfig auth;

    public FoxtrotServerConfiguration() {
        this.hbase = new HbaseConfig();
        this.elasticsearch = new ElasticsearchConfig();
        this.cluster = new ClusterConfig();
        this.deletionManagerConfig = new DataDeletionManagerConfig();
        this.emailConfig = new EmailConfig();
        this.segregationConfiguration = new SegregationConfiguration();
        this.restrictAccess = true;
        this.clusterRerouteConfig = new ClusterRerouteConfig();
    }

}

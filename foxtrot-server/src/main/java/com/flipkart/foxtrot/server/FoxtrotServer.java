/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.flipkart.foxtrot.core.common.DataDeletionManager;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.server.cluster.ClusterManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.server.providers.FlatResponseCsvProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseErrorTextProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseTextProvider;
import com.flipkart.foxtrot.server.resources.*;
import com.flipkart.foxtrot.server.util.ManagedActionScanner;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.google.common.collect.Lists;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.cobertura.CoverageIgnore;
import org.eclipse.jetty.servlets.CrossOriginFilter;

import javax.servlet.DispatcherType;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:38 PM
 */

@CoverageIgnore
public class FoxtrotServer extends Application<FoxtrotServerConfiguration> {
    @Override
    public void initialize(Bootstrap<FoxtrotServerConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/console/", "/"));
        bootstrap.addCommand(new InitializerCommand());
    }

    @Override
    public String getName() {
        return "foxtrot";
    }

    @Override
    public void run(FoxtrotServerConfiguration configuration, Environment environment) throws Exception {
        environment.getObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        environment.getObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        environment.getObjectMapper().setSubtypeResolver(subtypeResolver);

        ObjectMapper objectMapper = environment.getObjectMapper();
        ExecutorService executorService = environment.lifecycle().scheduledExecutorService("query-executor-%s", true).build();

        HbaseConfig hbaseConfig = configuration.getHbase();
        HbaseTableConnection HBaseTableConnection = new HbaseTableConnection(hbaseConfig);

        ElasticsearchConfig elasticsearchConfig = configuration.getElasticsearch();
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(elasticsearchConfig);

        ClusterConfig clusterConfig = configuration.getCluster();
        HazelcastConnection hazelcastConnection = new HazelcastConnection(clusterConfig, objectMapper);

        ElasticsearchUtils.setMapper(objectMapper);

        DataStore dataStore = new HBaseDataStore(HBaseTableConnection, objectMapper);


        TableMetadataManager tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection);
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
        FoxtrotTableManager tableManager = new FoxtrotTableManager(tableMetadataManager, queryStore, dataStore);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);

        QueryExecutor executor = new QueryExecutor(analyticsLoader, executorService);

        DataDeletionManagerConfig dataDeletionManagerConfig = configuration.getDeletionManagerConfig();
        DataDeletionManager dataDeletionManager = new DataDeletionManager(dataDeletionManagerConfig, queryStore);

        List<HealthCheck> healthChecks = Lists.newArrayList();
        healthChecks.add(new ElasticSearchHealthCheck("ES Health Check", elasticsearchConnection));

        int httpPort = 0;
        SimpleServerFactory serverFactory = (SimpleServerFactory) configuration.getServerFactory();
        HttpConnectorFactory connector = (HttpConnectorFactory) serverFactory.getConnector();
        if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
            httpPort = connector.getPort();
        }

        ClusterManager clusterManager = new ClusterManager(
                                    hazelcastConnection, healthChecks, httpPort);

        environment.lifecycle().manage(HBaseTableConnection);
        environment.lifecycle().manage(elasticsearchConnection);
        environment.lifecycle().manage(hazelcastConnection);
        environment.lifecycle().manage(tableMetadataManager);
        environment.lifecycle().manage(new ManagedActionScanner(analyticsLoader, environment));
        environment.lifecycle().manage(dataDeletionManager);
        environment.lifecycle().manage(clusterManager);

        environment.jersey().register(new DocumentResource(queryStore));
        environment.jersey().register(new AsyncResource());
        environment.jersey().register(new AnalyticsResource(executor));
        environment.jersey().register(new TableManagerResource(tableManager));
        environment.jersey().register(new TableFieldMappingResource(queryStore));
        environment.jersey().register(new ConsoleResource(
                new ElasticsearchConsolePersistence(elasticsearchConnection, objectMapper)));
        FqlEngine fqlEngine = new FqlEngine(tableMetadataManager, queryStore, executor, objectMapper);
        environment.jersey().register(new FqlResource(fqlEngine));
        environment.jersey().register(new ClusterInfoResource(clusterManager));
        environment.jersey().register(new UtilResource(configuration));

        for(HealthCheck healthCheck : healthChecks) {
            environment.healthChecks().register(healthCheck.getClass().getName(), healthCheck);
        }

        environment.jersey().register(new FlatResponseTextProvider());
        environment.jersey().register(new FlatResponseCsvProvider());
        environment.jersey().register(new FlatResponseErrorTextProvider());

        environment.servlets().addFilter("CrossOriginFilter", new CrossOriginFilter())
                .addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    }
}

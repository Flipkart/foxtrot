/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server;

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
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.assets.AssetsBundle;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.metrics.core.HealthCheck;
import net.sourceforge.cobertura.CoverageIgnore;
import org.eclipse.jetty.servlets.CrossOriginFilter;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:38 PM
 */

@CoverageIgnore
public class FoxtrotServer extends Service<FoxtrotServerConfiguration> {
    @Override
    public void initialize(Bootstrap<FoxtrotServerConfiguration> bootstrap) {
        bootstrap.setName("foxtrot");
        bootstrap.addBundle(new AssetsBundle("/console/", "/"));
        bootstrap.addCommand(new InitializerCommand());
    }

    @Override
    public void run(FoxtrotServerConfiguration configuration, Environment environment) throws Exception {
        configuration.getHttpConfiguration().setRootPath("/foxtrot/*");
        environment.getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        environment.getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        environment.getObjectMapperFactory().setSubtypeResolver(subtypeResolver);

        ObjectMapper objectMapper = environment.getObjectMapperFactory().build();
        ExecutorService executorService = environment.managedExecutorService("query-executor-%s", 20, 40, 30, TimeUnit.SECONDS);

        HbaseConfig hbaseConfig = configuration.getHbase();
        HbaseTableConnection HBaseTableConnection = new HbaseTableConnection(hbaseConfig);

        ElasticsearchConfig elasticsearchConfig = configuration.getElasticsearch();
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(elasticsearchConfig);

        ClusterConfig clusterConfig = configuration.getCluster();
        HazelcastConnection hazelcastConnection = new HazelcastConnection(clusterConfig, objectMapper);

        ElasticsearchUtils.setMapper(objectMapper);
        ElasticsearchUtils.setTableNamePrefix(elasticsearchConfig.getTableNamePrefix()); // Setting up tableName prefix.

        TableMetadataManager tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection,
                elasticsearchConnection);
        DataStore dataStore = new HBaseDataStore(HBaseTableConnection, objectMapper);
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
        FoxtrotTableManager tableManager = new FoxtrotTableManager(tableMetadataManager, queryStore, dataStore);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection);

        QueryExecutor executor = new QueryExecutor(analyticsLoader, executorService);

        DataDeletionManagerConfig dataDeletionManagerConfig = configuration.getTableDataManagerConfig();
        DataDeletionManager dataDeletionManager = new DataDeletionManager(dataDeletionManagerConfig, queryStore);

        List<HealthCheck> healthChecks = Lists.newArrayList();
        healthChecks.add(new ElasticSearchHealthCheck("ES Health Check", elasticsearchConnection));

        ClusterManager clusterManager = new ClusterManager(
                hazelcastConnection, healthChecks, configuration.getHttpConfiguration().getPort());

        environment.manage(HBaseTableConnection);
        environment.manage(elasticsearchConnection);
        environment.manage(hazelcastConnection);
        environment.manage(tableMetadataManager);
        environment.manage(new ManagedActionScanner(analyticsLoader, environment));
        environment.manage(dataDeletionManager);
        environment.manage(clusterManager);

        environment.addResource(new DocumentResource(queryStore));
        environment.addResource(new AsyncResource());
        environment.addResource(new AnalyticsResource(executor));
        environment.addResource(new TableManagerResource(tableManager));
        environment.addResource(new TableFieldMappingResource(queryStore));
        environment.addResource(new ConsoleResource(
                new ElasticsearchConsolePersistence(elasticsearchConnection, objectMapper)));
        FqlEngine fqlEngine = new FqlEngine(tableMetadataManager, queryStore, executor, objectMapper);
        environment.addResource(new FqlResource(fqlEngine));
        environment.addResource(new ClusterInfoResource(clusterManager));
        environment.addResource(new UtilResource(configuration));

        for (HealthCheck healthCheck : healthChecks) {
            environment.addHealthCheck(healthCheck);
        }

        environment.addProvider(new FlatResponseTextProvider());
        environment.addProvider(new FlatResponseCsvProvider());
        environment.addProvider(new FlatResponseErrorTextProvider());

        environment.addFilter(CrossOriginFilter.class, "/*");
    }
}

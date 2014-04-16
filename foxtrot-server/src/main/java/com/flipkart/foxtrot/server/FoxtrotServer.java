package com.flipkart.foxtrot.server;

import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.resources.*;
import com.flipkart.foxtrot.server.util.ManagedActionScanner;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:38 PM
 */
public class FoxtrotServer extends Service<FoxtrotServerConfiguration> {
    @Override
    public void initialize(Bootstrap<FoxtrotServerConfiguration> bootstrap) {
        bootstrap.setName("foxtrot");
    }

    @Override
    public void run(FoxtrotServerConfiguration configuration, Environment environment) throws Exception {
        environment.getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        environment.getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        environment.getObjectMapperFactory().setSubtypeResolver(subtypeResolver);

        ObjectMapper objectMapper = environment.getObjectMapperFactory().build();
        ExecutorService executorService = environment.managedExecutorService("query-executor-%s", 20,40, 30, TimeUnit.SECONDS);

        HbaseConfig hbaseConfig = configuration.getHbase();
        HbaseTableConnection hbaseTableConnection = new HbaseTableConnection(hbaseConfig);
        environment.manage(hbaseTableConnection);

        ElasticsearchConfig elasticsearchConfig = configuration.getElasticsearch();
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(elasticsearchConfig);
        environment.manage(elasticsearchConnection);

        ClusterConfig clusterConfig = configuration.getCluster();
        HazelcastConnection hazelcastConnection = new HazelcastConnection(clusterConfig, objectMapper);
        environment.manage(hazelcastConnection);


        DataStore dataStore = new HbaseDataStore(hbaseTableConnection, objectMapper);
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        environment.manage(new ManagedActionScanner(analyticsLoader, environment));

        TableMetadataManager tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection);
        environment.manage(tableMetadataManager);

        QueryExecutor executor = new QueryExecutor(analyticsLoader, executorService);
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, executor);

        environment.addResource(new DocumentResource(queryStore));
        environment.addResource(new QueryResource(queryStore));
        environment.addResource(new HistogramResource(queryStore));
        environment.addResource(new GroupResource(queryStore));
        environment.addResource(new AsyncResource());
        environment.addResource(new AnalyticsResource(executor));
        environment.addResource(new TableMetadataResource(tableMetadataManager));
    }
}

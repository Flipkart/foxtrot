package com.flipkart.foxtrot.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

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
        ObjectMapper objectMapper = environment.getObjectMapperFactory().build();

        HbaseConfig hbaseConfig = configuration.getHbase();
        HbaseTableConnection hbaseTableConnection = new HbaseTableConnection(hbaseConfig);
        environment.manage(hbaseTableConnection);

        ElasticsearchConfig elasticsearchConfig = configuration.getElasticsearch();
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(elasticsearchConfig);
        environment.manage(elasticsearchConnection);

        ClusterConfig clusterConfig = new ClusterConfig();
        HazelcastConnection hazelcastConnection = new HazelcastConnection(clusterConfig);
        environment.manage(hazelcastConnection);


        DataStore dataStore = new HbaseDataStore(hbaseTableConnection, objectMapper);
        QueryStore queryStore = new ElasticsearchQueryStore(elasticsearchConnection, dataStore, objectMapper);

    }
}

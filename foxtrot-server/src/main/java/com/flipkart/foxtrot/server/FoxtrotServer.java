package com.flipkart.foxtrot.server;

import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.impl.ClusterConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
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
        HbaseConfig hbaseConfig = configuration.getHbase();
        HbaseTableConnection hbaseTableConnection = new HbaseTableConnection(hbaseConfig);

        ElasticsearchConfig elasticsearchConfig = configuration.getElasticsearch();
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(elasticsearchConfig);

        ClusterConfig clusterConfig = new ClusterConfig();
        HazelcastConnection hazelcastConnection = new HazelcastConnection(clusterConfig);

        environment.manage(hbaseTableConnection);
        environment.manage(elasticsearchConnection);
        environment.manage(hazelcastConnection);


    }
}

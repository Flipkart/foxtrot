package com.flipkart.foxtrot.core.querystore.impl;

import com.yammer.dropwizard.lifecycle.Managed;
import net.sourceforge.cobertura.CoverageIgnore;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:38 AM
 */

@CoverageIgnore
public class ElasticsearchConnection implements Managed {
    private final ElasticsearchConfig config;
    private Client client;

    public ElasticsearchConnection(ElasticsearchConfig config) {
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", config.getCluster()).build();

        TransportClient esClient = new TransportClient(settings);
        for (String host : config.getHosts()) {
            esClient.addTransportAddress(
                    new InetSocketTransportAddress(host, 9300));
        }
        client = esClient;
    }

    @Override
    public void stop() throws Exception {
        client = null;
    }

    public Client getClient() {
        return client;
    }

    public ElasticsearchConfig getConfig() {
        return config;
    }
}

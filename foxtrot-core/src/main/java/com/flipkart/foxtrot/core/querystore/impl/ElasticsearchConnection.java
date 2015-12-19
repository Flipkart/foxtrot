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
package com.flipkart.foxtrot.core.querystore.impl;

import com.yammer.dropwizard.lifecycle.Managed;
import net.sourceforge.cobertura.CoverageIgnore;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:38 AM
 */

@CoverageIgnore
public class ElasticsearchConnection implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConnection.class.getSimpleName());
    private final ElasticsearchConfig config;
    private Client client;

    public ElasticsearchConnection(ElasticsearchConfig config) {
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Elasticsearch Client");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", config.getCluster()).build();

        TransportClient esClient = new TransportClient(settings);
        for (String host : config.getHosts()) {
            esClient.addTransportAddress(
                    new InetSocketTransportAddress(host, 9300));
            logger.info(String.format("Added Elasticsearch Node : %s", host));
        }
        client = esClient;
        logger.info("Started Elasticsearch Client");
    }

    @Override
    public void stop() throws Exception {
        if (client != null) {
            client.close();
        }
        client = null;
    }

    public Client getClient() {
        return client;
    }

    public ElasticsearchConfig getConfig() {
        return config;
    }
}

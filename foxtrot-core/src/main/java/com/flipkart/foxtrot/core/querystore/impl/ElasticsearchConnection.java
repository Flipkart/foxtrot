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
package com.flipkart.foxtrot.core.querystore.impl;

import io.dropwizard.lifecycle.Managed;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:38 AM
 */
@Singleton
@Order(5)
public class ElasticsearchConnection implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConnection.class.getSimpleName());
    @Getter
    private final ElasticsearchConfig config;
    @Getter
    private RestHighLevelClient client;

    @Inject
    public ElasticsearchConnection(ElasticsearchConfig config) {
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting ElasticSearch Client");
/*        Settings settings = Settings.builder()
                .put("cluster.name", config.getCluster())
                .put("client.transport.ignore_cluster_name", true)
                .build();

        TransportClient esClient = new CustomESTransportClient(settings);
        final int port = config.getPort() == null
            ? 9300
            : config.getPort();*/

       client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(config.getHosts().get(0), config.getPort(), "http")));
/*
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));
*/

/*        for(String host : config.getHosts()) {
            String[] tokenizedHosts = host.split(",");
            for(String tokenizedHost : tokenizedHosts) {
                esClient.addTransportAddress(new TransportAddress(InetAddress.getByName(tokenizedHost), port));
                logger.info("Added ElasticSearch Node : {}", host);
            }
        }
        client = esClient;*/
        logger.info("Started ElasticSearch Client");
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping ElasticSearch client");
        if(client != null) {
            client.close();
        }
        client = null;
    }

    @SneakyThrows
    public void refresh(final String index) {
        client
                .indices()
                .refresh(new RefreshRequest().indices(index));
    }
}

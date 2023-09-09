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

import com.flipkart.foxtrot.core.querystore.impl.OpensearchConfig.ConnectionType;
import io.dropwizard.lifecycle.Managed;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.http.HttpHost;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 12:38 AM
 */
@Singleton
@Order(5)
public class OpensearchConnection implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(OpensearchConnection.class.getSimpleName());
    @Getter
    private final OpensearchConfig config;
    @Getter
    private RestHighLevelClient client;

    @Inject
    public OpensearchConnection(OpensearchConfig config) {
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Opensearch Client");
        final int defaultPort = config.getConnectionType() == ConnectionType.HTTP
                                ? 80
                                : 443;
        int port = config.getPort() == 0
                   ? defaultPort
                   : config.getPort();
        val hosts = config.getHosts()
                .stream()
                .map(host -> {
                    final String scheme = config.getConnectionType() == ConnectionType.HTTP
                                          ? "http"
                                          : "https";
                    return new HttpHost(host, port, scheme);
                })
                .toArray(HttpHost[]::new);
        client = new RestHighLevelClient(RestClient.builder(hosts));
        logger.info("Started Opensearch Client");
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping Opensearch client");
        if (client != null) {
            client.close();
        }
        client = null;
    }

    @SneakyThrows
    public void refresh(final String index) {
        client.indices()
                .refresh(new RefreshRequest().indices(index), RequestOptions.DEFAULT);
    }
}

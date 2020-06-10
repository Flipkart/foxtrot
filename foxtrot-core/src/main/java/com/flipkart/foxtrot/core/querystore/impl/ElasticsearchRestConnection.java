package com.flipkart.foxtrot.core.querystore.impl;

import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig.ConnectionType;
import io.dropwizard.lifecycle.Managed;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

/***
 User : nitish.goyal
 Date : 10/06/20
 Time : 4:43 PM
 ***/
@Singleton
@Order(5)
public class ElasticsearchRestConnection implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(
            com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection.class.getSimpleName());
    @Getter
    private final ElasticsearchConfig config;
    @Getter
    private RestHighLevelClient client;

    @Inject
    public ElasticsearchRestConnection(ElasticsearchConfig config) {
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting ElasticSearch Client");
        final int defaultPort = config.getConnectionType() == ConnectionType.HTTP
                                ? 80
                                : 443;
        int port = config.getPort() == null
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
        logger.info("Started ElasticSearch Client");
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping ElasticSearch client");
        if (client != null) {
            client.close();
        }
        client = null;
    }

    @SneakyThrows
    public void refresh(final String index) {
        client.indices()
                .refresh(new RefreshRequest().indices(index));
    }
}

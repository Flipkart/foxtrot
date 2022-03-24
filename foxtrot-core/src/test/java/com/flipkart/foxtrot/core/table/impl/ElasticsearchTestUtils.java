package com.flipkart.foxtrot.core.table.impl;
/*
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
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

import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import io.appform.testcontainers.elasticsearch.config.ElasticsearchContainerConfiguration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;

import java.util.Collections;

import static io.appform.testcontainers.commons.ContainerUtils.containerLogsConsumer;
import static io.appform.testcontainers.elasticsearch.utils.ElasticsearchContainerUtils.getCompositeWaitStrategy;
import static io.appform.testcontainers.elasticsearch.utils.ElasticsearchContainerUtils.getJavaOpts;

/***
 Created by nitish.goyal on 02/08/18
 ***/
@Slf4j
public class ElasticsearchTestUtils {

    public static synchronized ElasticsearchConnection getConnection() throws Exception {
        // To make sure we load class which will start the server.
        ElasticsearchContainerHolder.containerLoaded = true;
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(
                ElasticsearchContainerHolder.getElasticsearchConfig());
        elasticsearchConnection.start();

        return elasticsearchConnection;
    }

    public static void cleanupIndices(final ElasticsearchConnection elasticsearchConnection) {
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("_all");
            final AcknowledgedResponse deleteIndexResponse = elasticsearchConnection.getClient()
                    .indices()
                    .delete(deleteIndexRequest, RequestOptions.DEFAULT);
            log.info("Delete index response: {}", deleteIndexResponse);
        } catch (Exception e) {
            log.error("Index Cleanup failed", e);
        }
    }

    /**
     * Class to make sure we run the server only once.
     */
    private static class ElasticsearchContainerHolder {

        @SuppressWarnings("unused")
        private static boolean containerLoaded;

        @Getter
        private static ElasticsearchConfig elasticsearchConfig;

        static {
            try {
                ElasticsearchContainerConfiguration configuration = new ElasticsearchContainerConfiguration();
                configuration.setDockerImage("docker.elastic.co/elasticsearch/elasticsearch-oss:6.8.8");
                configuration.setClusterRamMb(100);
                GenericContainer esContainer = new FixedHostPortGenericContainer(configuration.getDockerImage())
                        .withExposedPorts(configuration.getHttpPort(), configuration.getTransportPort())
                        .withEnv("cluster.name", "elasticsearch")
                        .withEnv("discovery.type", "single-node")
                        .withEnv("ES_JAVA_OPTS", getJavaOpts(configuration))
                        .withLogConsumer(containerLogsConsumer(log))
                        .waitingFor(getCompositeWaitStrategy(configuration))
                        .withStartupTimeout(configuration.getTimeoutDuration());
                esContainer.start();

                Integer mappedPort = esContainer.getMappedPort(configuration.getHttpPort());

                elasticsearchConfig = new ElasticsearchConfig();
                elasticsearchConfig.setHosts(Collections.singletonList(configuration.getHost()));
                elasticsearchConfig.setPort(mappedPort);
                elasticsearchConfig.setConnectionType(ElasticsearchConfig.ConnectionType.HTTP);
                elasticsearchConfig.setCluster("elasticsearch");
                elasticsearchConfig.setTableNamePrefix("foxtrot");
            } catch (Exception e) {
                log.error("Error in initializing es test container , error :", e);
                throw e;
            }
        }
    }
}

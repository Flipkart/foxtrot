package com.flipkart.foxtrot.core.table.impl;
/*
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

import static com.phonepe.testcontainer.commons.ContainerUtils.containerLogsConsumer;
import static com.phonepe.testcontainer.elasticsearch.utils.ElasticsearchContainerUtils.getCompositeWaitStrategy;
import static com.phonepe.testcontainer.elasticsearch.utils.ElasticsearchContainerUtils.getJavaOpts;

import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.phonepe.testcontainer.elasticsearch.config.ElasticsearchContainerConfiguration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;

import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;

/***
 Created by nitish.goyal on 02/08/18
 ***/
public class ElasticsearchTestUtils {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTestUtils.class.getSimpleName());

    /**
     * Class to make sure we run the server only once.
     */
    private static class ElasticsearchContainerHolder {

        @SuppressWarnings("unused")
        static int loadContainer = 0;

        @Getter
        private static ElasticsearchConfig elasticsearchConfig;

        static {
            try {
                ElasticsearchContainerConfiguration configuration = new ElasticsearchContainerConfiguration();
                configuration.setClusterRamMb(100);
                GenericContainer esContainer = new FixedHostPortGenericContainer(configuration.getDockerImage())
                        .withExposedPorts(configuration.getHttpPort(), configuration.getTransportPort())
                        .withEnv("cluster.name", "elasticsearch")
                        .withEnv("discovery.type", "single-node")
                        .withEnv("ES_JAVA_OPTS", getJavaOpts(configuration))
                        .withLogConsumer(containerLogsConsumer(logger))
                        .waitingFor(getCompositeWaitStrategy(configuration))
                        .withStartupTimeout(configuration.getTimeoutDuration());
                esContainer.start();

                Integer mappedPort = esContainer.getMappedPort(configuration.getTransportPort());

                elasticsearchConfig = new ElasticsearchConfig();
                elasticsearchConfig.setHosts(Collections.singletonList(configuration.getHost()));
                elasticsearchConfig.setPort(mappedPort);
                elasticsearchConfig.setCluster("elasticsearch");
                elasticsearchConfig.setTableNamePrefix("foxtrot");
            } catch (Exception e) {
                logger.error("Error in initializing es test container , error :", e);
            }
        }
    }

    public static ElasticsearchConnection getConnection() throws Exception {
        // To make sure we load class which will start the server.
        ElasticsearchContainerHolder.loadContainer = 1;
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(
                ElasticsearchContainerHolder.getElasticsearchConfig());
        elasticsearchConnection.start();

        return elasticsearchConnection;
    }

    public static void cleanupIndices(final ElasticsearchConnection elasticsearchConnection) {
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("_all");
            final DeleteIndexResponse deleteIndexResponse = elasticsearchConnection.getClient()
                    .admin()
                    .indices()
                    .delete(deleteIndexRequest)
                    .get();
            logger.info("Delete index response: {}", deleteIndexResponse);
        } catch (Exception e) {
            logger.error("Index Cleanup failed", e);
        }
    }
}

/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.foxtrot.server;

import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseUtil;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.impl.TableMapStore;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreServiceImpl;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitializerCommand extends ConfiguredCommand<FoxtrotServerConfiguration> {

    private static final Logger logger = LoggerFactory.getLogger(InitializerCommand.class.getSimpleName());

    public InitializerCommand() {
        super("initialize", "Initialize elasticsearch and hbase");
    }

    @Override
    protected void run(Bootstrap<FoxtrotServerConfiguration> bootstrap,
                       Namespace namespace,
                       FoxtrotServerConfiguration configuration) throws Exception {
        ElasticsearchConfig esConfig = configuration.getElasticsearch();
        ElasticsearchConnection connection = new ElasticsearchConnection(esConfig);
        connection.start();

        ClusterHealthResponse clusterHealth = connection.getClient()
                .cluster()
                .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numDataNodes = clusterHealth.getNumberOfDataNodes();
        int numReplicas = (numDataNodes < 2)
                          ? 0
                          : 1;

        logger.info("# data nodes: {}, Setting replica count to: {}", numDataNodes, numReplicas);

        createMetaIndex(connection, ElasticsearchConsolePersistence.INDEX, numReplicas);
        createMetaIndex(connection, ElasticsearchConsolePersistence.INDEX_V2, numReplicas);
        createMetaIndex(connection, TableMapStore.TABLE_META_INDEX, numReplicas);
        createMetaIndex(connection, ElasticsearchConsolePersistence.INDEX_HISTORY, numReplicas);
        createMetaIndex(connection, FqlStoreServiceImpl.FQL_STORE_INDEX, numReplicas);
        createMetaIndex(connection, "user-meta", numReplicas);
        createMetaIndex(connection, "tokens", numReplicas);

        logger.info("Creating mapping");
        PutIndexTemplateRequest putIndexTemplateRequest = ElasticsearchUtils.getClusterTemplateMapping();
        AcknowledgedResponse response = connection.getClient()
                .indices()
                .putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);
        logger.info("Created mapping: {}", response.isAcknowledged());

        logger.info("Creating hbase table");
        HBaseUtil.createTable(configuration.getHbase(), configuration.getHbase()
                .getTableName());
    }

    private void createMetaIndex(final ElasticsearchConnection connection,
                                 final String indexName,
                                 int replicaCount) {
        try {
            logger.info("'{}' creation started", indexName);
            Settings settings = Settings.builder()
                    .put("number_of_shards", 1)
                    .put("number_of_replicas", replicaCount)
                    .build();
            CreateIndexRequest createIndexRequest = new CreateIndexRequest().index(indexName)
                    .settings(settings);
            CreateIndexResponse response = connection.getClient()
                    .indices()
                    .create(createIndexRequest, RequestOptions.DEFAULT);
            logger.info("'{}' creation acknowledged: {}", indexName, response.isAcknowledged());
            if (!response.isAcknowledged()) {
                logger.error("Index {} could not be created.", indexName);
            }
        } catch (Exception e) {
            if (null != e.getCause()) {
                logger.error("Index {} could not be created: {}", indexName, e.getCause()
                        .getLocalizedMessage());
            } else {
                logger.error("Index {} could not be created: {}", indexName, e.getLocalizedMessage());
            }
        }
    }


}

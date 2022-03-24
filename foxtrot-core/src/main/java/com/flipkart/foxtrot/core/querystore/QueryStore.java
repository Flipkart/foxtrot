/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore;

import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.common.elasticsearch.index.IndexInfoResponse;
import com.flipkart.foxtrot.common.elasticsearch.node.NodeFSStatsResponse;
import com.flipkart.foxtrot.core.rebalance.ShardInfoResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:25 PM
 */
public interface QueryStore {

    void initializeTable(final Table table);

    void initializeTenant(final String tenant);

    void updateTable(final String tableName,
                     final Table table);

    void save(final String table,
              final Document document);

    void saveAll(final String table,
                 final List<Document> documents);

    Document get(final String table,
                 final String id);

    List<Document> getAll(final String table,
                          final List<String> ids);

    List<Document> getAll(final String table,
                          final List<String> ids,
                          boolean bypassMetaLookup);

    Set<String> getTablesIndices();

    List<ShardInfoResponse> getTableShardsInfo();

    List<IndexInfoResponse> getTableIndicesInfo();

    void cleanupAll();

    void cleanup(final String table);

    void cleanup(final Set<String> tables);

    ClusterHealthResponse getClusterHealth();

    JsonNode getNodeStats();

    NodeFSStatsResponse getNodeFSStats();

    JsonNode getIndicesStats() throws IOException;

    TableFieldMapping getFieldMappings(String table);

}

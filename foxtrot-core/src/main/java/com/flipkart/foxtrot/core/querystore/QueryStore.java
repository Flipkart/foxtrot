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
package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:25 PM
 */
public interface QueryStore {

    void initializeTable(final String table) throws FoxtrotException;

    void save(final String table, final Document document) throws FoxtrotException;

    void save(final String table, final List<Document> documents) throws FoxtrotException;

    Document get(final String table, final String id) throws FoxtrotException;

    List<Document> getAll(final String table, final List<String> ids) throws FoxtrotException;

    List<Document> getAll(final String table, final List<String> ids, boolean bypassMetaLookup) throws FoxtrotException;

    TableFieldMapping getFieldMappings(final String table) throws FoxtrotException;

    void cleanupAll() throws FoxtrotException;

    void cleanup(final String table) throws FoxtrotException;

    void cleanup(final Set<String> tables) throws FoxtrotException;

    ClusterHealthResponse getClusterHealth() throws ExecutionException, InterruptedException;

    NodesStatsResponse getNodeStats() throws ExecutionException, InterruptedException;

    IndicesStatsResponse getIndicesStats() throws ExecutionException, InterruptedException;
}

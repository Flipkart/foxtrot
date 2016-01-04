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
package com.flipkart.foxtrot.server.console;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Vector;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

public class ElasticsearchConsolePersistence implements ConsolePersistence {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConsolePersistence.class);
    private static final String INDEX = "consoles";
    private static final String TYPE = "console_data";

    private ElasticsearchConnection connection;
    private ObjectMapper mapper;

    public ElasticsearchConsolePersistence(ElasticsearchConnection connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    public void save(final Console console) throws FoxtrotException {
        try {
            connection.getClient()
                    .prepareIndex()
                    .setIndex(INDEX)
                    .setType(TYPE)
                    .setId(console.getId())
                    .setSource(mapper.writeValueAsBytes(console))
                    .setRefresh(true)
                    .setConsistencyLevel(WriteConsistencyLevel.ALL)
                    .execute()
                    .get();
            logger.info(String.format("Saved Console : %s", console));
        } catch (Exception e) {
            throw new ConsolePersistenceException(console.getId(), "console save failed", e);
        }
    }

    @Override
    public Console get(final String id) throws FoxtrotException {
        try {
            GetResponse result = connection.getClient()
                    .prepareGet()
                    .setIndex(INDEX)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            if (!result.isExists()) {
                return null;
            }
            return mapper.readValue(result.getSourceAsBytes(), Console.class);
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "console save failed", e);
        }
    }

    @Override
    public void delete(final String id) throws FoxtrotException {
        try {
            connection.getClient()
                    .prepareDelete()
                    .setConsistencyLevel(WriteConsistencyLevel.ALL)
                    .setRefresh(true)
                    .setIndex(INDEX)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            logger.info(String.format("Deleted Console : %s", id));
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "console deletion_failed", e);
        }
    }

    @Override
    public List<Console> get() throws FoxtrotException {
        SearchResponse response = connection.getClient().prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(boolQuery().must(matchAllQuery()))
                .addSort(fieldSort("name").order(SortOrder.DESC))
                .setScroll(new TimeValue(60000))
                .setSearchType(SearchType.SCAN)
                .execute()
                .actionGet();
        try {
            Vector<Console> results = new Vector<Console>();
            while (true) {
                response = connection.getClient().prepareSearchScroll(response.getScrollId())
                        .setScroll(new TimeValue(60000))
                        .execute()
                        .actionGet();
                SearchHits hits = response.getHits();
                for (SearchHit hit : hits) {
                    results.add(mapper.readValue(hit.sourceAsString(), Console.class));
                }
                if (0 == response.getHits().hits().length) {
                    break;
                }
            }
            return results;
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }
}

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
package com.flipkart.foxtrot.server.console;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.Vector;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

public class ElasticsearchConsolePersistence implements ConsolePersistence {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConsolePersistence.class);
    private static final String INDEX = "consoles";
    private static final String INDEX_V2 = "consoles_v2";
    private static final String TYPE = "console_data";
    private static final String INDEX_HISTORY = "consoles_history";
    private static final int SCROLL_SIZE = 500;

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
                    .setSource(ElasticsearchQueryUtils.getSourceMap(console, console.getClass()))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
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
            if(!result.isExists()) {
                return null;
            }
            return mapper.readValue(result.getSourceAsBytes(), Console.class);
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "console save failed", e);
        }
    }

    @Override
    public List<Console> get() throws FoxtrotException {
        SearchResponse response = connection.getClient()
                .prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(boolQuery().must(matchAllQuery()))
                .addSort(fieldSort("name").order(SortOrder.DESC))
                .setScroll(new TimeValue(60000))
                .execute()
                .actionGet();
        try {
            Vector<Console> results = new Vector<Console>();
            while (true) {
                response = connection.getClient()
                        .prepareSearchScroll(response.getScrollId())
                        .setScroll(new TimeValue(60000))
                        .execute()
                        .actionGet();
                SearchHits hits = response.getHits();
                    for(SearchHit hit : hits) {
                        results.add(mapper.readValue(hit.getSourceAsString(), Console.class));
                    }
                    if(0 == response.getHits()
                            .getHits().length) {
                        break;
                    }
                } return results;
        } catch(Exception e){
            throw new ConsoleFetchException(e);
        }
    }

    @Override public void delete ( final String id) throws FoxtrotException {
        try {
            connection.getClient()
                    .prepareDelete()
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
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

    @Override public void saveV2 (ConsoleV2 console, boolean newConsole) throws FoxtrotException {
        preProcess(console, newConsole);
        try {
            connection.getClient()
                    .prepareIndex()
                    .setIndex(INDEX_V2)
                    .setType(TYPE)
                    .setId(console.getId())
                    .setSource(mapper.writeValueAsBytes(console), XContentType.JSON)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute()
                    .get();
            logger.info(String.format("Saved Console : %s", console));
        } catch (Exception e) {
            throw new ConsolePersistenceException(console.getId(), "console save failed", e);
        }
    }

    @Override public ConsoleV2 getV2 (String id) throws FoxtrotException {
        try {
            GetResponse result = connection.getClient()
                    .prepareGet()
                    .setIndex(INDEX_V2)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            if(!result.isExists()) {
                return null;
            }
            return mapper.readValue(result.getSourceAsBytes(), ConsoleV2.class);
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "console save failed", e);
        }
    }

    @Override public List<ConsoleV2> getV2 () throws FoxtrotException {
        SearchResponse response = connection.getClient()
                .prepareSearch(INDEX_V2)
                .setTypes(TYPE)
                .setQuery(boolQuery().must(matchAllQuery()))
                .setSize(SCROLL_SIZE)
                .addSort(fieldSort("name").order(SortOrder.DESC))
                .setScroll(new TimeValue(60000))
                .execute()
                .actionGet();
        try {
            Vector<ConsoleV2> results = new Vector<ConsoleV2>();
            while (true) {
                SearchHits hits = response.getHits();
                for(SearchHit hit : hits) {
                    results.add(mapper.readValue(hit.getSourceAsString(), ConsoleV2.class));
                }
                response = connection.getClient()
                        .prepareSearchScroll(response.getScrollId())
                        .setScroll(new TimeValue(60000))
                        .execute()
                        .actionGet();
                if(0 == response.getHits()
                        .getHits().length) {
                    break;
                }
            }
            return results;
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }

    @Override
    public List<ConsoleV2> getAllOldVersions (final String name) throws FoxtrotException {
        String updatedAt = "updatedAt";
        try {
            SearchHits searchHits = connection.getClient()
                    .prepareSearch(INDEX_HISTORY)
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.termQuery("name.keyword", name))
                    .addSort(SortBuilders
                            .fieldSort(updatedAt)
                            .order(SortOrder.DESC))
                    .setFrom(0)
                    .setSize(10)
                    .execute()
                    .actionGet()
                    .getHits();
            Vector<ConsoleV2> results = new Vector<ConsoleV2>();
            for(SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchHits.getHits())) {
                results.add(mapper.readValue(searchHit.getSourceAsString(), ConsoleV2.class));
            }
            return results;
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }

    @Override
    public ConsoleV2 getOldVersion (final String id) throws FoxtrotException {
        try {
            GetResponse result = connection.getClient()
                    .prepareGet()
                    .setIndex(INDEX_HISTORY)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            if(!result.isExists()) {
                return null;
            }
            return mapper.readValue(result.getSourceAsBytes(), ConsoleV2.class);
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }

    @Override
    public void setOldVersionAsCurrent (final String id) throws FoxtrotException {
        try {
            ConsoleV2 console = getOldVersion(id);
            saveV2(console, false);
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }

    private void preProcess (ConsoleV2 console, boolean newConsole) throws FoxtrotException {
        if(console.getUpdatedAt() == 0L) {
            console.setUpdatedAt(System.currentTimeMillis());
        }
        ConsoleV2 oldConsole = getV2(console.getId());
        if(oldConsole == null){
            oldConsole = getOldVersion(console.getId());
            //In this case old Console Id (random Id) is passed therefore changing the id to current console Id
            if(oldConsole != null) {
                String id = oldConsole.getName().replaceAll("\\s+", "_").toLowerCase();
                oldConsole = getV2(id);
                console.setId(id);
            }
        }
        if(oldConsole == null){
            return;
        }
        if(oldConsole.getUpdatedAt() != 0L && oldConsole.getUpdatedAt() > console.getUpdatedAt() && newConsole) {
            throw new ConsolePersistenceException(console.getId(),
                                                  "Updated version of console exists. Kindly refresh" +
                                                  " your dashboard"
            );
        }
        if(oldConsole.getVersion() == 0) {
            oldConsole.setVersion(1);
        }
        saveOldConsole(oldConsole);
        console.setUpdatedAt(System.currentTimeMillis());
        console.setVersion(oldConsole.getVersion() + 1);
    }

    @Override public void deleteV2 (String id) throws FoxtrotException {
        try {
            connection.getClient()
                    .prepareDelete()
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setIndex(INDEX_V2)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            logger.info(String.format("Deleted Console : %s", id));
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "console deletion_failed", e);
        }
    }

    @Override public void deleteOldVersion (String id) throws FoxtrotException {
        try {
            connection.getClient()
                    .prepareDelete()
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setIndex(INDEX_HISTORY)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            logger.info(String.format("Deleted Console : %s", id));
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "console deletion_failed", e);
        }
    }

    private void saveOldConsole (ConsoleV2 console) throws FoxtrotException {
        String id = UUID.randomUUID().toString();
        console.setId(id);
        try {
            connection.getClient()
                    .prepareIndex()
                    .setIndex(INDEX_HISTORY)
                    .setType(TYPE)
                    .setId(id)
                    .setSource(mapper.writeValueAsBytes(console), XContentType.JSON)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute()
                    .get();
            logger.info(String.format("Saved Old Console : %s", console));
        } catch (Exception e) {
            throw new ConsolePersistenceException(console.getId(), "old console save failed", e);
        }
    }
}

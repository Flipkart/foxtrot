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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
    private static final long SCROLL_TIMEOUT = TimeUnit.MINUTES.toMillis(2);

    private ElasticsearchConnection connection;
    private ObjectMapper mapper;

    public ElasticsearchConsolePersistence(ElasticsearchConnection connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    public void save(final Console console) {
        try {
            connection.getClient()
                    .prepareIndex()
                    .setIndex(INDEX)
                    .setType(TYPE)
                    .setId(console.getId())
                    .setSource(ElasticsearchQueryUtils.toMap(mapper, console))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute()
                    .get();
            logger.info("Saved Console : {}", console);
        } catch (Exception e) {
            throw new ConsolePersistenceException(console.getId(), "console save failed", e);
        }
    }

    @Override
    public Console get(final String id) {
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
            throw new ConsolePersistenceException(id, "console get failed", e);
        }
    }

    @Override
    public List<Console> get() {
        SearchResponse response = connection.getClient()
                .prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(boolQuery().must(matchAllQuery()))
                .addSort(fieldSort("name").order(SortOrder.DESC))
                .setScroll(new TimeValue(60000))
                .execute()
                .actionGet();
        try {
            List<Console> results = new ArrayList<>();
            while (true) {
                response = connection.getClient()
                        .prepareSearchScroll(response.getScrollId())
                        .setScroll(new TimeValue(60000))
                        .execute()
                        .actionGet();
                SearchHits hits = response.getHits();
                for (SearchHit hit : hits) {
                    results.add(mapper.readValue(hit.getSourceAsString(), Console.class));
                }
                if (0 == response.getHits()
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
    public void delete(final String id) {
        try {
            connection.getClient()
                    .prepareDelete()
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setIndex(INDEX)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            logger.info("Deleted Console : {}", id);
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "console deletion_failed", e);
        }
    }

    @Override
    public void saveV2(ConsoleV2 console, boolean newConsole) {
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
            logger.info("Saved Console : {}", console);
        } catch (Exception e) {
            throw new ConsolePersistenceException(console.getId(), "console save failed", e);
        }
    }

    @Override
    public ConsoleV2 getV2(String id) {
        try {
            GetResponse result = connection.getClient()
                    .prepareGet()
                    .setIndex(INDEX_V2)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            if (!result.isExists()) {
                return null;
            }
            return mapper.readValue(result.getSourceAsBytes(), ConsoleV2.class);
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "console get failed", e);
        }
    }

    @Override
    public List<ConsoleV2> getV2() {
        SearchResponse response = connection.getClient()
                .prepareSearch(INDEX_V2)
                .setTypes(TYPE)
                .setQuery(boolQuery().must(matchAllQuery()))
                .setSize(SCROLL_SIZE)
                .addSort(fieldSort("name.keyword").order(SortOrder.DESC))
                .setScroll(new TimeValue(SCROLL_TIMEOUT))
                .execute()
                .actionGet();
        try {
            List<ConsoleV2> results = new ArrayList<>();
            while (true) {
                SearchHits hits = response.getHits();
                for (SearchHit hit : hits) {
                    results.add(mapper.readValue(hit.getSourceAsString(), ConsoleV2.class));
                }
                if (SCROLL_SIZE >= response.getHits()
                        .getTotalHits().value) {
                    break;
                }

                response = connection.getClient()
                        .prepareSearchScroll(response.getScrollId())
                        .setScroll(new TimeValue(SCROLL_TIMEOUT))
                        .execute()
                        .actionGet();
            }
            return results;
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }

    @Override
    public List<ConsoleV2> getAllOldVersions(final String name, final String sortBy) {
        try {
            SearchHits searchHits = connection.getClient()
                    .prepareSearch(INDEX_HISTORY)
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.termQuery("name.keyword", name))
                    .addSort(SortBuilders.fieldSort(sortBy)
                            .order(SortOrder.DESC))
                    .setFrom(0)
                    .setSize(10)
                    .execute()
                    .actionGet()
                    .getHits();
            List<ConsoleV2> results = new ArrayList<>();
            for (SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchHits.getHits())) {
                results.add(mapper.readValue(searchHit.getSourceAsString(), ConsoleV2.class));
            }
            return results;
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }

    @Override
    public ConsoleV2 getOldVersion(final String id) {
        try {
            GetResponse result = connection.getClient()
                    .prepareGet()
                    .setIndex(INDEX_HISTORY)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            if (!result.isExists()) {
                return null;
            }
            return mapper.readValue(result.getSourceAsBytes(), ConsoleV2.class);
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }

    @Override
    public void setOldVersionAsCurrent(final String id) {
        try {
            ConsoleV2 console = getOldVersion(id);
            saveV2(console, false);
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }

    private void preProcess(ConsoleV2 console, boolean newConsole) {
        if (console.getUpdatedAt() == 0L) {
            console.setUpdatedAt(System.currentTimeMillis());
        }
        ConsoleV2 oldConsole = getV2(console.getId());
        if (oldConsole == null) {
            oldConsole = getOldVersion(console.getId());
            //In this case old Console Id (random Id) is passed therefore changing the id to current console Id
            if (oldConsole != null) {
                String id = oldConsole.getName()
                        .replaceAll("\\s+", "_")
                        .toLowerCase();
                oldConsole = getV2(id);
                console.setId(id);
            }
        }
        if (oldConsole == null) {
            console.setVersion(1);
            return;
        }
        if (oldConsole.getUpdatedAt() != 0L && oldConsole.getUpdatedAt() > console.getUpdatedAt() && newConsole) {
            throw new ConsolePersistenceException(console.getId(), "Updated version of console exists. Kindly refresh" + " your dashboard");
        }

        String sortBy = "version";
        List<ConsoleV2> consoleV2s;
        int maxOldConsoleVersion = 0;
        consoleV2s = getAllOldVersions(oldConsole.getName(), sortBy);
        if (consoleV2s != null && !consoleV2s.isEmpty()) {
            maxOldConsoleVersion = consoleV2s.get(0)
                    .getVersion();
        }

        int oldCurrentConsoleVersion = oldConsole.getVersion();
        int version = Math.max(oldCurrentConsoleVersion, maxOldConsoleVersion);
        if (oldCurrentConsoleVersion > maxOldConsoleVersion) {
            saveOldConsole(oldConsole);
        }
        console.setUpdatedAt(System.currentTimeMillis());
        console.setVersion(version + 1);
    }

    @Override
    public void deleteV2(String id) {
        try {
            connection.getClient()
                    .prepareDelete()
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setIndex(INDEX_V2)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            logger.info("Deleted Console : {}", id);
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "console deletion_failed", e);
        }
    }

    @Override
    public void deleteOldVersion(String id) {
        try {
            connection.getClient()
                    .prepareDelete()
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setIndex(INDEX_HISTORY)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            logger.info("Deleted Old Console : {}", id);
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "old console deletion_failed", e);
        }
    }

    private void saveOldConsole(ConsoleV2 console) {
        String id = UUID.randomUUID()
                .toString();
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
            logger.info("Saved Old Console : {}", console);
        } catch (Exception e) {
            throw new ConsolePersistenceException(console.getId(), "old console save failed", e);
        }
    }
}

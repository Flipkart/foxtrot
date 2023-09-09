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

import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.sort.SortBuilders.fieldSort;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import com.flipkart.foxtrot.core.util.OpensearchQueryUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class OpensearchConsolePersistence implements ConsolePersistence {
    public static final String INDEX_HISTORY = "consoles_history";
    public static final String INDEX_V2 = "consoles_v2";
    public static final String INDEX = "consoles";

    private static final Logger logger = LoggerFactory.getLogger(OpensearchConsolePersistence.class);
    private static final String TYPE = "console_data";
    private static final int SCROLL_SIZE = 500;
    private static final long SCROLL_TIMEOUT = TimeUnit.MINUTES.toMillis(2);

    private OpensearchConnection connection;
    private ObjectMapper mapper;

    @Inject
    public OpensearchConsolePersistence(OpensearchConnection connection,
                                        ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    public void save(final Console console) {
        try {
            connection.getClient()
                    .index(new IndexRequest(INDEX).id(console.getId()).source(OpensearchQueryUtils.toMap(mapper, console))
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
            logger.info("Saved Console : {}", console);
        } catch (Exception e) {
            throw new ConsolePersistenceException(console.getId(), "console save failed", e);
        }
    }

    @Override
    public Console get(final String id) {
        try {
            GetResponse result = connection.getClient()
                    .get(new GetRequest(INDEX, id), RequestOptions.DEFAULT);
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
        SearchResponse response = null;
        try {
            response = connection.getClient()
                    .search(new SearchRequest(INDEX)
                                    .source(new SearchSourceBuilder()
                                            .query(boolQuery().must(matchAllQuery()))
                                            .sort(fieldSort("name").order(SortOrder.DESC)))
                                    .scroll(new TimeValue(60000)),
                            RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ConsolePersistenceException("", "console listing failed", e);
        }
        try {
            List<Console> results = new ArrayList<>();
            while (true) {
                response = connection.getClient()
                        .scroll(new SearchScrollRequest(response.getScrollId())
                                        .scroll(new TimeValue(60000)),
                                RequestOptions.DEFAULT);
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
                    .delete(new DeleteRequest()
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                            .index(INDEX)
                            .id(id), RequestOptions.DEFAULT);
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
                    .index(new IndexRequest(INDEX_V2)
                                    .id(console.getId())
                                    .source(mapper.writeValueAsBytes(console), XContentType.JSON)
                                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                            RequestOptions.DEFAULT);
            logger.info("Saved Console : {}", console);
        } catch (Exception e) {
            throw new ConsolePersistenceException(console.getId(), "console save failed", e);
        }
    }

    @Override
    public ConsoleV2 getV2(String id) {
        try {
            GetResponse result = connection.getClient()
                    .get(new GetRequest(INDEX_V2, id), RequestOptions.DEFAULT);
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
        try {
            SearchResponse response = connection.getClient()
                    .search(new SearchRequest(INDEX_V2)
                            .source(new SearchSourceBuilder()
                                    .query(boolQuery().must(matchAllQuery()))
                                    .size(SCROLL_SIZE)
                                    .sort(fieldSort("name.keyword").order(SortOrder.DESC).unmappedType("keyword")))
                            .scroll(new TimeValue(SCROLL_TIMEOUT)), RequestOptions.DEFAULT);
            List<ConsoleV2> results = new ArrayList<>();
            while (true) {
                SearchHits hits = response.getHits();
                for (SearchHit hit : hits) {
                    results.add(mapper.readValue(hit.getSourceAsString(), ConsoleV2.class));
                }
                if (SCROLL_SIZE >= response.getHits()
                        .getHits().length) {
                    break;
                }

                response = connection.getClient()
                        .scroll(new SearchScrollRequest(response.getScrollId())
                                .scroll(new TimeValue(SCROLL_TIMEOUT)), RequestOptions.DEFAULT);
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
                    .search(new SearchRequest(INDEX_HISTORY)
                                    .searchType(SearchType.QUERY_THEN_FETCH)
                                    .source(new SearchSourceBuilder()
                                            .query(QueryBuilders.termQuery("name.keyword", name))
                                            .sort(SortBuilders.fieldSort(sortBy).order(SortOrder.DESC))
                                            .from(0)
                                            .size(10)),
                            RequestOptions.DEFAULT)
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
                    .get(new GetRequest(INDEX_HISTORY, id), RequestOptions.DEFAULT);
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
                    .delete(new DeleteRequest(INDEX_V2, id).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                            RequestOptions.DEFAULT);
            logger.info("Deleted Console : {}", id);
        } catch (Exception e) {
            throw new ConsolePersistenceException(id, "console deletion_failed", e);
        }
    }

    @Override
    public void deleteOldVersion(String id) {
        try {
            connection.getClient()
                    .delete(new DeleteRequest(INDEX_HISTORY, id).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                            RequestOptions.DEFAULT);
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
                    .index(new IndexRequest(INDEX_HISTORY).id(id).source(mapper.writeValueAsBytes(console), XContentType.JSON)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
            logger.info("Saved Old Console : {}", console);
        } catch (Exception e) {
            throw new ConsolePersistenceException(console.getId(), "old console save failed", e);
        }
    }
}

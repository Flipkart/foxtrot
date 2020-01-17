package com.flipkart.foxtrot.server.jobs.consolehistory;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.config.ConsoleHistoryConfig;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.server.console.ConsoleFetchException;
import com.flipkart.foxtrot.server.console.ConsoleV2;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;

/***
 Created by mudit.g on Dec, 2018
 ***/
@Singleton
@Order(45)
public class ConsoleHistoryManager extends BaseJobManager {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleHistoryManager.class.getSimpleName());
    private static final String TYPE = "console_data";
    private static final String INDEX_V2 = "consoles_v2";
    private static final String INDEX_HISTORY = "consoles_history";
    private final ElasticsearchConnection connection;
    private final ConsoleHistoryConfig consoleHistoryConfig;
    private final ObjectMapper mapper;
    private final ElasticsearchConsolePersistence elasticsearchConsolePersistence;

    @Inject
    public ConsoleHistoryManager(ScheduledExecutorService scheduledExecutorService, ConsoleHistoryConfig consoleHistoryConfig,
                                 ElasticsearchConnection connection, HazelcastConnection hazelcastConnection, ObjectMapper mapper) {
        super(consoleHistoryConfig, scheduledExecutorService, hazelcastConnection);
        this.consoleHistoryConfig = consoleHistoryConfig;
        this.connection = connection;
        this.mapper = mapper;
        this.elasticsearchConsolePersistence = new ElasticsearchConsolePersistence(connection, mapper);
    }

    @Override
    protected void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                SearchResponse searchResponse = connection.getClient()
                        .prepareSearch(INDEX_V2)
                        .setTypes(TYPE)
                        .setSearchType(SearchType.QUERY_THEN_FETCH)
                        .addAggregation(AggregationBuilders.terms("names")
                                                .field("name.keyword")
                                                .size(1000))
                        .execute()
                        .actionGet();
                Terms agg = searchResponse.getAggregations()
                        .get("names");
                for (Terms.Bucket entry : agg.getBuckets()) {
                    deleteOldData(entry.getKeyAsString());
                }
            }
            catch (Exception e) {
                logger.info("Failed to get aggregations and delete data for index history. {}", e);
            }

        }, new LockConfiguration(consoleHistoryConfig.getJobName(), lockAtMostUntil));
    }

    private void deleteOldData(final String name) {
        String updatedAt = "updatedAt";
        try {
            SearchHits searchHits = connection.getClient()
                    .prepareSearch(INDEX_HISTORY)
                    .setTypes(TYPE)
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.termQuery("name.keyword", name))
                    .addSort(SortBuilders.fieldSort(updatedAt)
                                     .order(SortOrder.DESC))
                    .setFrom(10)
                    .setSize(9000)
                    .execute()
                    .actionGet()
                    .getHits();
            for (SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchHits.getHits())) {
                ConsoleV2 consoleV2 = mapper.readValue(searchHit.getSourceAsString(), ConsoleV2.class);
                elasticsearchConsolePersistence.deleteOldVersion(consoleV2.getId());
            }
        }
        catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }
}

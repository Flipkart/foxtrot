package com.flipkart.foxtrot.server.jobs.consolehistory;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import com.flipkart.foxtrot.server.console.ConsoleFetchException;
import com.flipkart.foxtrot.server.console.ConsoleV2;
import com.flipkart.foxtrot.server.console.OpensearchConsolePersistence;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Inject;
import javax.inject.Singleton;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

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
    private final OpensearchConnection connection;
    private final ConsoleHistoryConfig consoleHistoryConfig;
    private final ObjectMapper mapper;
    private final OpensearchConsolePersistence opensearchConsolePersistence;

    @Inject
    public ConsoleHistoryManager(ScheduledExecutorService scheduledExecutorService,
                                 ConsoleHistoryConfig consoleHistoryConfig,
                                 OpensearchConnection connection,
                                 HazelcastConnection hazelcastConnection,
                                 ObjectMapper mapper) {
        super(consoleHistoryConfig, scheduledExecutorService, hazelcastConnection);
        this.consoleHistoryConfig = consoleHistoryConfig;
        this.connection = connection;
        this.mapper = mapper;
        this.opensearchConsolePersistence = new OpensearchConsolePersistence(connection, mapper);
    }

    @Override
    protected void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                SearchResponse searchResponse = connection.getClient()
                        .search(new SearchRequest(INDEX_V2)
                                .searchType(SearchType.QUERY_THEN_FETCH)
                                .source(new SearchSourceBuilder()
                                        .aggregation(AggregationBuilders.terms("names")
                                                .field("name.keyword")
                                                .size(1000))), RequestOptions.DEFAULT);

                Terms agg = searchResponse.getAggregations()
                        .get("names");
                for (Terms.Bucket entry : agg.getBuckets()) {
                    deleteOldData(entry.getKeyAsString());
                }
            } catch (Exception e) {
                logger.info("Failed to get aggregations and delete data for index history. {}", e);
            }

        }, new LockConfiguration(consoleHistoryConfig.getJobName(), lockAtMostUntil));
    }

    private void deleteOldData(final String name) {
        String updatedAt = "updatedAt";
        try {
            SearchHits searchHits = connection.getClient()
                    .search(new SearchRequest(INDEX_HISTORY)
                                    .searchType(SearchType.QUERY_THEN_FETCH)
                                    .source(new SearchSourceBuilder()
                                            .query(QueryBuilders.termQuery("name.keyword", name))
                                            .sort(SortBuilders.fieldSort(updatedAt).order(SortOrder.DESC))
                                            .from(10)
                                            .size(9000)),
                            RequestOptions.DEFAULT)
                    .getHits();
            for (SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchHits.getHits())) {
                ConsoleV2 consoleV2 = mapper.readValue(searchHit.getSourceAsString(), ConsoleV2.class);
                opensearchConsolePersistence.deleteOldVersion(consoleV2.getId());
            }
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }
}

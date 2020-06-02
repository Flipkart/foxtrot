package com.flipkart.foxtrot.core.jobs.optimization;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.val;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

/***
 Created by nitish.goyal on 11/09/18
 ***/
@Singleton
@Order(40)
public class EsIndexOptimizationManager extends BaseJobManager {

    private static final int BATCH_SIZE = 5;
    private static final int SEGMENTS_TO_OPTIMIZE_TO = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(EsIndexOptimizationManager.class.getSimpleName());

    private final ElasticsearchConnection elasticsearchConnection;
    private final EsIndexOptimizationConfig esIndexOptimizationConfig;

    @Inject
    public EsIndexOptimizationManager(ScheduledExecutorService scheduledExecutorService,
            EsIndexOptimizationConfig esIndexOptimizationConfig, ElasticsearchConnection elasticsearchConnection,
            HazelcastConnection hazelcastConnection) {
        super(esIndexOptimizationConfig, scheduledExecutorService, hazelcastConnection);
        this.esIndexOptimizationConfig = esIndexOptimizationConfig;
        this.elasticsearchConnection = elasticsearchConnection;
    }

    @Override
    protected void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                val indexes = elasticsearchConnection.getClient()
                        .indices()
                        .get(new GetIndexRequest("*"), RequestOptions.DEFAULT)
                        .getIndices();
                val candidateIndices = Arrays.stream(indexes)
                        .filter(index -> {
                            String table = ElasticsearchUtils.getTableNameFromIndex(index);
                            if (Strings.isNullOrEmpty(table)) {
                                return false;
                            }
                            String currentIndex = ElasticsearchUtils.getCurrentIndex(table, System.currentTimeMillis());
                            String nextDayIndex = ElasticsearchUtils.getCurrentIndex(table,
                                    System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1));
                            if (index.equals(currentIndex) || index.equals(nextDayIndex)) {
                                return false;
                            }
                            return true;
                        })
                        .collect(Collectors.toSet());
                List<List<String>> batchOfIndicesToOptimize = CollectionUtils.partition(candidateIndices, BATCH_SIZE);
                for (List<String> indices : batchOfIndicesToOptimize) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    elasticsearchConnection.getClient()
                            .indices()
                            .forcemerge(new ForceMergeRequest(indices.toArray(new String[0])).maxNumSegments(
                                    SEGMENTS_TO_OPTIMIZE_TO)
                                    .flush(true)
                                    .onlyExpungeDeletes(false), RequestOptions.DEFAULT);
                    LOGGER.info("No of indexes optimized : {}", indices.size());
                    MetricUtil.getInstance()
                            .registerActionSuccess("indexesOptimized", CollectionUtils.mkString(indices, ","),
                                    stopwatch.elapsed(TimeUnit.MILLISECONDS));
                }
            } catch (IOException e) {
                LOGGER.error("Error getting index list", e);
            }
        }, new LockConfiguration(esIndexOptimizationConfig.getJobName(), lockAtMostUntil));
    }

}

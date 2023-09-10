package com.flipkart.foxtrot.core.jobs.optimization;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchUtils;
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
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

/***
 Created by nitish.goyal on 11/09/18
 ***/
@Singleton
@Order(40)
public class IndexOptimizationManager extends BaseJobManager {

    private static final int BATCH_SIZE = 5;
    private static final int SEGMENTS_TO_OPTIMIZE_TO = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexOptimizationManager.class.getSimpleName());

    private final OpensearchConnection opensearchConnection;
    private final DatabaseIndexOptimizationConfig indexOptimizationConfig;

    @Inject
    public IndexOptimizationManager(ScheduledExecutorService scheduledExecutorService,
                                    DatabaseIndexOptimizationConfig indexOptimizationConfig,
                                    OpensearchConnection opensearchConnection,
                                    HazelcastConnection hazelcastConnection) {
        super(indexOptimizationConfig, scheduledExecutorService, hazelcastConnection);
        this.indexOptimizationConfig = indexOptimizationConfig;
        this.opensearchConnection = opensearchConnection;
    }

    @Override
    protected void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                val indexes = opensearchConnection.getClient()
                        .indices()
                        .get(new GetIndexRequest("*"), RequestOptions.DEFAULT)
                        .getIndices();
                val candidateIndices = Arrays.stream(indexes)
                        .filter(index -> {
                            String table = OpensearchUtils.getTableNameFromIndex(index);
                            if (Strings.isNullOrEmpty(table)) {
                                return false;
                            }
                            String currentIndex = OpensearchUtils.getCurrentIndex(table, System.currentTimeMillis());
                            String nextDayIndex = OpensearchUtils.getCurrentIndex(table,
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
                    opensearchConnection.getClient()
                            .indices()
                            .forcemerge(new ForceMergeRequest(indices.toArray(new String[0]))
                                    .maxNumSegments(SEGMENTS_TO_OPTIMIZE_TO)
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
        }, new LockConfiguration(indexOptimizationConfig.getJobName(), lockAtMostUntil));
    }


    /*@Override
    protected void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                IndicesSegmentsRequest indicesSegmentsRequest = new IndicesSegmentsRequest();

                IndicesSegmentResponse indicesSegmentResponse = opensearchConnection.getClient()
                        .admin()
                        .indices()
                        .forcemerge()
                        .segments(indicesSegmentsRequest)
                        .actionGet();
                Set<String> indicesToOptimize = Sets.newHashSet();

                Map<String, IndexSegments> segmentResponseIndices = indicesSegmentResponse.getIndices();
                for(Map.Entry<String, IndexSegments> entry : segmentResponseIndices.entrySet()) {
                    String index = entry.getKey();
                    extractIndicesToOptimizeForIndex(index, entry.getValue(), indicesToOptimize);
                }
                optimizeIndices(indicesToOptimize);
                LOGGER.info("No of indexes optimized : {}", indicesToOptimize.size());
            } catch (Exception e) {
                LOGGER.error("Error occurred while calling optimization API", e);
            }
        }, new LockConfiguration(esIndexOptimizationConfig.getJobName(), lockAtMostUntil));
    }

    private void optimizeIndices(Set<String> indicesToOptimize) {
        List<List<String>> batchOfIndicesToOptimize = CollectionUtils.partition(indicesToOptimize, BATCH_SIZE);
        for(List<String> indices : batchOfIndicesToOptimize) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            opensearchConnection.getClient()
                    .admin()
                    .indices()
                    .prepareForceMerge(indices.toArray(new String[0]))
                    .setMaxNumSegments(SEGMENTS_TO_OPTIMIZE_TO)
                    .setFlush(true)
                    .setOnlyExpungeDeletes(false)
                    .execute()
                    .actionGet();
            LOGGER.info("No of indexes optimized : {}", indices.size());
            MetricUtil.getInstance()
                    .registerActionSuccess("indexesOptimized", CollectionUtils.mkString(indices, ","),
                                           stopwatch.elapsed(TimeUnit.MILLISECONDS)
                                          );
        }
    }

    private void extractIndicesToOptimizeForIndex(String index, IndexSegments indexShardSegments, Set<String> indicesToOptimize) {

        String table = OpensearchUtils.getTableNameFromIndex(index);
        if(StringUtils.isEmpty(table)) {
            return;
        }
        String currentIndex = OpensearchUtils.getCurrentIndex(table, System.currentTimeMillis());
        String nextDayIndex = OpensearchUtils.getCurrentIndex(table, System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1));
        if(index.equals(currentIndex) || index.equals(nextDayIndex)) {
            return;
        }
        Map<Integer, IndexShardSegments> indexShardSegmentsMap = indexShardSegments.getShards();
        for(Map.Entry<Integer, IndexShardSegments> indexShardSegmentsEntry : indexShardSegmentsMap.entrySet()) {
            List<Segment> segments = indexShardSegmentsEntry.getValue()
                    .iterator()
                    .next()
                    .getSegments();
            if(segments.size() > SEGMENTS_TO_OPTIMIZE_TO) {
                indicesToOptimize.add(index);
                break;
            }
        }
    }*/

}

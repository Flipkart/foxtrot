package com.flipkart.foxtrot.core.jobs.optimization;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.index.engine.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 Created by nitish.goyal on 11/09/18
 ***/
public class EsIndexOptimizationManager extends BaseJobManager {

    private static final int BATCH_SIZE = 5;
    private static final int SEGMENTS_TO_OPTIMIZE_TO = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(EsIndexOptimizationManager.class.getSimpleName());

    private final ElasticsearchConnection elasticsearchConnection;
    private final EsIndexOptimizationConfig esIndexOptimizationConfig;

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
                IndicesSegmentsRequest indicesSegmentsRequest = new IndicesSegmentsRequest();
                IndicesSegmentResponse indicesSegmentResponse = elasticsearchConnection.getClient()
                        .admin()
                        .indices()
                        .segments(indicesSegmentsRequest)
                        .actionGet();
                Set<String> indicesToOptimize = Sets.newHashSet();

                Map<String, IndexSegments> segmentResponseIndices = indicesSegmentResponse.getIndices();
                for (Map.Entry<String, IndexSegments> entry : segmentResponseIndices.entrySet()) {
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

    private void extractIndicesToOptimizeForIndex(String index, IndexSegments indexShardSegments,
            Set<String> indicesToOptimize) {

        String table = ElasticsearchUtils.getTableNameFromIndex(index);
        if (StringUtils.isEmpty(table)) {
            return;
        }
        String currentIndex = ElasticsearchUtils.getCurrentIndex(table, System.currentTimeMillis());
        String nextDayIndex = ElasticsearchUtils.getCurrentIndex(table, System.currentTimeMillis() +
                TimeUnit.DAYS.toMillis(1));
        if (index.equals(currentIndex) || index.equals(nextDayIndex)) {
            return;
        }
        Map<Integer, IndexShardSegments> indexShardSegmentsMap = indexShardSegments.getShards();
        for (Map.Entry<Integer, IndexShardSegments> indexShardSegmentsEntry : indexShardSegmentsMap.entrySet()) {
            List<Segment> segments = indexShardSegmentsEntry.getValue()
                    .iterator()
                    .next()
                    .getSegments();
            if (segments.size() > SEGMENTS_TO_OPTIMIZE_TO) {
                indicesToOptimize.add(index);
                break;
            }
        }
    }

    private void optimizeIndices(Set<String> indicesToOptimize) {
        List<List<String>> batchOfIndicesToOptimize = CollectionUtils.partition(indicesToOptimize, BATCH_SIZE);
        for (List<String> indices : batchOfIndicesToOptimize) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            elasticsearchConnection.getClient()
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
                            stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }
}

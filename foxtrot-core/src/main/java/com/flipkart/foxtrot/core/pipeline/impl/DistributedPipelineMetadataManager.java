package com.flipkart.foxtrot.core.pipeline.impl;

import com.flipkart.foxtrot.core.pipeline.PipelineMetadataManager;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.pipeline.Pipeline;
import com.google.common.collect.Lists;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.IMap;
import io.appform.functionmetrics.MonitoredFunction;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;


@Singleton
@Slf4j
public class DistributedPipelineMetadataManager implements PipelineMetadataManager {

    private static final String PIPELINE_DATA_MAP = "pipelinemetadatamap";

    private static final int TIME_TO_LIVE_PIPELINE_CACHE = (int) TimeUnit.DAYS.toSeconds(30);


    private final HazelcastConnection hazelcastConnection;
    private final ElasticsearchConnection elasticsearchConnection;
    private IMap<String, Pipeline> pipelineDataStore;

    @Inject
    public DistributedPipelineMetadataManager(HazelcastConnection hazelcastConnection,
                                              ElasticsearchConnection elasticsearchConnection) {
        this.hazelcastConnection = hazelcastConnection;
        this.elasticsearchConnection = elasticsearchConnection;

        hazelcastConnection.getHazelcastConfig()
                .addMapConfig(pipelineMapConfig());
    }


    private MapConfig pipelineMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(PIPELINE_DATA_MAP);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.setReadBackupData(true);
        mapConfig.setTimeToLiveSeconds(TIME_TO_LIVE_PIPELINE_CACHE);
        mapConfig.setBackupCount(0);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setFactoryImplementation(PipelineMapStore.factory(elasticsearchConnection));
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setTimeToLiveSeconds(TIME_TO_LIVE_PIPELINE_CACHE);
        nearCacheConfig.setInvalidateOnChange(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        return mapConfig;
    }

    @Override
    @MonitoredFunction
    public void save(Pipeline pipeline) {
        pipelineDataStore.put(pipeline.getName(), pipeline);
        pipelineDataStore.flush();
    }

    @Override
    @MonitoredFunction
    public Pipeline get(String pipelineName) {
        if (pipelineDataStore.containsKey(pipelineName)) {
            return pipelineDataStore.get(pipelineName);
        }
        return null;
    }


    @Override
    @SneakyThrows
    public List<Pipeline> get() {
        if (0 == pipelineDataStore.size()) {
            return Collections.emptyList();
        }
        ArrayList<Pipeline> pipelines = Lists.newArrayList(pipelineDataStore.values());
        pipelines.sort(Comparator.comparing(pipeline -> pipeline.getName()
                .toLowerCase()));
        return pipelines;
    }


    @Override
    public boolean exists(String pipelineName) {
        return pipelineDataStore.containsKey(pipelineName);
    }

    @Override
    public void start() {
        pipelineDataStore = hazelcastConnection.getHazelcast()
                .getMap(PIPELINE_DATA_MAP);
    }

    @Override
    public void stop() {
        //do nothing
    }

}

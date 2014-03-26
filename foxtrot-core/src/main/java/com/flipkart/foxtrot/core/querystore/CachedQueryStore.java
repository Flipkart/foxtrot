package com.flipkart.foxtrot.core.querystore;

import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.core.common.ActionResponse;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 23/03/14
 * Time: 11:38 PM
 */
public class CachedQueryStore implements QueryStore {
    private static final Logger logger = LoggerFactory.getLogger(CachedQueryStore.class.getSimpleName());
    private static final String MAP_NAME = "CacheMap";

    private QueryStore queryStoreImpl;
    private IMap<String, Object> cache;

    public CachedQueryStore(QueryStore queryStoreImpl, HazelcastConnection hazelcastConnection) {
        this.queryStoreImpl = queryStoreImpl;
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setEvictionPolicy(MapConfig.EvictionPolicy.LFU);
        mapConfig.setTimeToLiveSeconds(30);
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        hazelcastConnection.getHazelcast().getConfig().setMapConfigs(Collections.singletonMap(MAP_NAME, mapConfig));
        cache = hazelcastConnection.getHazelcast().getMap(MAP_NAME);

    }

    @Override
    public void save(String table, Document document) throws QueryStoreException {
        queryStoreImpl.save(table, document);
    }

    @Override
    public void save(String table, List<Document> documents) throws QueryStoreException {
        queryStoreImpl.save(table, documents);
    }

    @Override
    public Document get(String table, String id) throws QueryStoreException {
        final String key = String.format("%s-%s", table, id);
        if(!cache.containsKey(key)) {
            Document document = queryStoreImpl.get(table, id);
            if(null != document) {
                cache.put(key, document);
            }
            else {
                return null;
            }
        }
        Object cached = cache.get(key);
        if(cached instanceof Document) {
            return (Document) cached;
        }
        else {
            logger.error("Found cached object, but not a document. Key collision!!: " + key);
        }
        return null;
    }

    @Override
    public List<Document> get(String table, List<String> ids) throws QueryStoreException {
        return null;
    }

    @Override
    public ActionResponse runQuery(Query query) throws QueryStoreException {
        return null;
    }

    @Override
    public AsyncDataToken runQueryAsync(Query query) throws QueryStoreException {
        return null;
    }

    @Override
    public JsonNode getDataForQuery(String queryId) throws QueryStoreException {
        return null;
    }

    @Override
    public HistogramResponse histogram(HistogramRequest histogramRequest) throws QueryStoreException {
        return null;
    }

    @Override
    public GroupResponse group(GroupRequest groupRequest) throws QueryStoreException {
        return null;
    }
}

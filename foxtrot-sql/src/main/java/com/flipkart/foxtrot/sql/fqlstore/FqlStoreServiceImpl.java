package com.flipkart.foxtrot.sql.fqlstore;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.DOCUMENT_TYPE_NAME;
import static com.flipkart.foxtrot.sql.fqlstore.FqlStore.TITLE;

/***
 Created by mudit.g on Jan, 2019
 ***/
public class FqlStoreServiceImpl implements FqlStoreService {
    private static final Logger logger = LoggerFactory.getLogger(FqlStore.class);
    private static final String FQL_STORE_INDEX = "foxtrot-fql-store";

    private final ElasticsearchConnection elasticsearchConnection;
    private final ObjectMapper objectMapper;

    public FqlStoreServiceImpl(ElasticsearchConnection elasticsearchConnection, ObjectMapper objectMapper) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.objectMapper = objectMapper;
    }

    @Override
    public void save(FqlStore fqlStore) {
        fqlStore.setId(UUID.randomUUID().toString());
        try {
            elasticsearchConnection.getClient()
                    .prepareIndex()
                    .setIndex(FQL_STORE_INDEX)
                    .setType(DOCUMENT_TYPE_NAME)
                    .setId(fqlStore.getId())
                    .setSource(objectMapper.writeValueAsBytes(fqlStore), XContentType.JSON)
                    .execute()
                    .get();
            logger.info(String.format("Saved FQL Query : %s", fqlStore.getQuery()));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Request couldn't be saved for FQL query: " + fqlStore.getQuery() + " Error Message: " + e);
        }
    }

    @Override
    public List<FqlStore> get(FqlGetRequest fqlGetRequest) throws Exception {
        SearchHits searchHits;
        List<FqlStore> fqlStoreList = new ArrayList<>();
        searchHits = elasticsearchConnection.getClient()
                .prepareSearch(FQL_STORE_INDEX)
                .setTypes(DOCUMENT_TYPE_NAME)
                .setQuery(QueryBuilders.prefixQuery(TITLE, fqlGetRequest.getTitle().toLowerCase()))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setFrom(fqlGetRequest.getFrom())
                .setSize(fqlGetRequest.getSize())
                .execute()
                .actionGet()
                .getHits();
        for(SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchHits.getHits())) {
            fqlStoreList.add(objectMapper.readValue(searchHit.getSourceAsString(), FqlStore.class));
        }
        return fqlStoreList;
    }
}

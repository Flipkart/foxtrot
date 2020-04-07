package com.flipkart.foxtrot.sql.fqlstore;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.exception.FqlPersistenceException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.DOCUMENT_TYPE_NAME;
import static com.flipkart.foxtrot.sql.fqlstore.FqlStore.TITLE_FIELD;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Singleton
public class FqlStoreServiceImpl implements FqlStoreService {
    public static final String FQL_STORE_INDEX = "fql-store";

    private static final Logger logger = LoggerFactory.getLogger(FqlStore.class);

    private final ElasticsearchConnection elasticsearchConnection;
    private final ObjectMapper objectMapper;

    @Inject
    public FqlStoreServiceImpl(ElasticsearchConnection elasticsearchConnection, ObjectMapper objectMapper) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.objectMapper = objectMapper;
    }

    @Override
    public void save(FqlStore fqlStore) {
        fqlStore.setId(UUID.randomUUID()
                               .toString());
        try {
            elasticsearchConnection.getClient()
                    .index(new IndexRequest(FQL_STORE_INDEX, DOCUMENT_TYPE_NAME, fqlStore.getId())
                        .source(objectMapper.writeValueAsBytes(fqlStore), XContentType.JSON), RequestOptions.DEFAULT);
            logger.info("Saved FQL Query : {}", fqlStore.getQuery());
        } catch (Exception e) {
            throw new FqlPersistenceException("Couldn't save FQL query: " + fqlStore.getQuery() + " Error Message: " + e.getMessage(), e);
        }
    }

    @Override
    public List<FqlStore> get(FqlGetRequest fqlGetRequest) {
        SearchHits searchHits;
        List<FqlStore> fqlStoreList = new ArrayList<>();
        try {
            searchHits = elasticsearchConnection.getClient()
                    .search(new SearchRequest(FQL_STORE_INDEX)
                        .types(DOCUMENT_TYPE_NAME)
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(new SearchSourceBuilder()
                               .query(QueryBuilders.prefixQuery(TITLE_FIELD, fqlGetRequest.getTitle().toLowerCase()))
                                .from(fqlGetRequest.getFrom())
                                .size(fqlGetRequest.getSize())),
                            RequestOptions.DEFAULT)
                    .getHits();
            for(SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchHits.getHits())) {
                fqlStoreList.add(objectMapper.readValue(searchHit.getSourceAsString(), FqlStore.class));
            }
        } catch (Exception e) {
            throw new FqlPersistenceException("Couldn't get FqlStore: " + e.getMessage(), e);
        }
        return fqlStoreList;
    }
}

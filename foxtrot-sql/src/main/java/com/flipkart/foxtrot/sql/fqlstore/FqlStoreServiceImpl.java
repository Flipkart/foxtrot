package com.flipkart.foxtrot.sql.fqlstore;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.exception.FqlPersistenceException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.DOCUMENT_TYPE_NAME;
import static com.flipkart.foxtrot.sql.fqlstore.FqlStore.TITLE_FIELD;
import static com.flipkart.foxtrot.sql.fqlstore.FqlStore.USER_ID;


/***
 Created by mudit.g on Jan, 2019
 ***/
@Singleton
@Slf4j
public class FqlStoreServiceImpl implements FqlStoreService {

    public static final String FQL_STORE_INDEX = "fql-store";
    private final ElasticsearchConnection elasticsearchConnection;
    private final ObjectMapper objectMapper;

    @Inject
    public FqlStoreServiceImpl(ElasticsearchConnection elasticsearchConnection,
                               ObjectMapper objectMapper) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.objectMapper = objectMapper;
    }

    @Override
    public void save(FqlStore fqlStore) {
        fqlStore.setId(UUID.randomUUID()
                .toString());
        try {
            elasticsearchConnection.getClient()
                    .index(new IndexRequest(FQL_STORE_INDEX, DOCUMENT_TYPE_NAME, fqlStore.getId()).source(
                            objectMapper.writeValueAsBytes(fqlStore), XContentType.JSON), RequestOptions.DEFAULT);
            log.info("Saved FQL Query : {}", fqlStore.getQuery());
        } catch (Exception e) {
            throw new FqlPersistenceException(
                    "Couldn't save FQL query: " + fqlStore.getQuery() + " Error Message: " + e.getMessage(), e);
        }
    }

    @Override
    public List<FqlStore> get(FqlGetRequest fqlGetRequest) {
        // case: when userId is not defined
        if (Strings.isNullOrEmpty(fqlGetRequest.getUserId())) {
            return new ArrayList<>();
        }
        SearchHits searchHits;
        List<FqlStore> fqlStoreList = new ArrayList<>();
        try {
            if (fqlGetRequest.getTitle()
                    .isEmpty()) {
                searchHits = elasticsearchConnection.getClient()
                        .search(new SearchRequest(FQL_STORE_INDEX).types(DOCUMENT_TYPE_NAME)
                                .searchType(SearchType.QUERY_THEN_FETCH)
                                .source(new SearchSourceBuilder().query(QueryBuilders.prefixQuery(USER_ID,
                                        fqlGetRequest.getUserId()
                                                .toLowerCase()))
                                        .from(fqlGetRequest.getFrom())
                                        .size(fqlGetRequest.getSize())), RequestOptions.DEFAULT)
                        .getHits();
            } else {
                searchHits = elasticsearchConnection.getClient()
                        .search(new SearchRequest(FQL_STORE_INDEX).types(DOCUMENT_TYPE_NAME)
                                .searchType(SearchType.QUERY_THEN_FETCH)
                                .source(new SearchSourceBuilder().query(QueryBuilders.prefixQuery(TITLE_FIELD,
                                        fqlGetRequest.getTitle()
                                                .toLowerCase()))
                                        .from(fqlGetRequest.getFrom())
                                        .size(fqlGetRequest.getSize())), RequestOptions.DEFAULT)
                        .getHits();
            }
            for (SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchHits.getHits())) {
                FqlStore fqlStore = objectMapper.readValue(searchHit.getSourceAsString(), FqlStore.class);
                if (!Strings.isNullOrEmpty(fqlStore.getUserId()) && Objects.equals(fqlStore.getUserId(),
                        fqlGetRequest.getUserId())) {
                    fqlStoreList.add(fqlStore);
                }
            }
        } catch (Exception e) {
            throw new FqlPersistenceException("Couldn't get FqlStore: " + e.getMessage(), e);
        }
        return fqlStoreList;
    }
}

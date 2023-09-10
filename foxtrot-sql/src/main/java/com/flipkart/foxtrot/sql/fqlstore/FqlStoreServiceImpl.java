package com.flipkart.foxtrot.sql.fqlstore;

import static com.flipkart.foxtrot.sql.fqlstore.FqlStore.TITLE_FIELD;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.exception.FqlPersistenceException;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Singleton
public class FqlStoreServiceImpl implements FqlStoreService {
    public static final String FQL_STORE_INDEX = "fql-store";

    private static final Logger logger = LoggerFactory.getLogger(FqlStore.class);

    private final OpensearchConnection opensearchConnection;
    private final ObjectMapper objectMapper;

    @Inject
    public FqlStoreServiceImpl(OpensearchConnection opensearchConnection,
                               ObjectMapper objectMapper) {
        this.opensearchConnection = opensearchConnection;
        this.objectMapper = objectMapper;
    }

    @Override
    public void save(FqlStore fqlStore) {
        fqlStore.setId(UUID.randomUUID()
                .toString());
        try {
            opensearchConnection.getClient()
                    .index(new IndexRequest(FQL_STORE_INDEX, fqlStore.getId()).source(
                            objectMapper.writeValueAsBytes(fqlStore), XContentType.JSON), RequestOptions.DEFAULT);
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
            searchHits = opensearchConnection.getClient()
                    .search(new SearchRequest(FQL_STORE_INDEX).searchType(SearchType.QUERY_THEN_FETCH)
                            .source(new SearchSourceBuilder().query(QueryBuilders.prefixQuery(TITLE_FIELD,
                                            fqlGetRequest.getTitle()
                                                    .toLowerCase()))
                                    .from(fqlGetRequest.getFrom())
                                    .size(fqlGetRequest.getSize())), RequestOptions.DEFAULT)
                    .getHits();
            for (SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchHits.getHits())) {
                fqlStoreList.add(objectMapper.readValue(searchHit.getSourceAsString(), FqlStore.class));
            }
        } catch (Exception e) {
            throw new FqlPersistenceException("Couldn't get FqlStore: " + e.getMessage(), e);
        }
        return fqlStoreList;
    }
}

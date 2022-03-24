package com.flipkart.foxtrot.sql.fqlstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FqlPersistenceException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import lombok.SneakyThrows;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.DOCUMENT_TYPE_NAME;
import static com.flipkart.foxtrot.sql.fqlstore.FqlStore.TITLE_FIELD;
import static com.flipkart.foxtrot.sql.fqlstore.FqlStore.USER_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class fqlStoreServiceImplTest {

    public String FQL_STORE_INDEX = "fql-store";
    private ElasticsearchConnection elasticsearchConnection;
    private ObjectMapper objectMapper;
    private FqlStoreServiceImpl fqlStoreServiceImpl;
    private RestHighLevelClient restHighLevelClient;
    private SearchResponse searchResponse;
    private SearchHits searchHits;
    private SearchHit searchHit;

    @Before
    public void setUp() throws Exception {
        this.elasticsearchConnection = mock(ElasticsearchConnection.class);
        this.objectMapper = new ObjectMapper();
        this.fqlStoreServiceImpl = new FqlStoreServiceImpl(elasticsearchConnection, objectMapper);
        this.restHighLevelClient = mock(RestHighLevelClient.class);
        this.searchResponse = mock(SearchResponse.class);
        this.searchHits = mock(SearchHits.class);
        this.searchHit = mock(SearchHit.class);
    }

    @Test
    public void testSaveFailure() throws Exception {
        doReturn(restHighLevelClient).when(elasticsearchConnection)
                .getClient();
        doThrow(new RuntimeException()).when(restHighLevelClient)
                .index(any(IndexRequest.class), any(RequestOptions.class));
        try {
            String title = "testQuery";
            String query = "select * from test";

            FqlStore fqlStore = new FqlStore();
            fqlStore.setTitle(title);
            fqlStore.setQuery(query);
            fqlStoreServiceImpl.save(fqlStore);
            fail();
        } catch (FqlPersistenceException e) {
            assertEquals(ErrorCode.FQL_PERSISTENCE_EXCEPTION, e.getCode());
        }
    }

    @SneakyThrows
    @Test
    public void testSaveSuccess() {

        String title = "testQuery";
        String userId = "userId";
        String query = "select * from test";

        FqlStore fqlStore = new FqlStore();
        fqlStore.setTitle(title);
        fqlStore.setId(userId);
        fqlStore.setQuery(query);
        doReturn(restHighLevelClient).when(elasticsearchConnection)
                .getClient();
        doReturn(mock(IndexResponse.class)).when(restHighLevelClient)
                .index(new IndexRequest(FQL_STORE_INDEX, DOCUMENT_TYPE_NAME, fqlStore.getId()).source(
                        objectMapper.writeValueAsBytes(fqlStore), XContentType.JSON), RequestOptions.DEFAULT);
        fqlStoreServiceImpl.save(fqlStore);
        Mockito.verify(restHighLevelClient, atLeast(1))
                .index(Mockito.any(IndexRequest.class), Mockito.any(RequestOptions.class));
    }

    @Test
    public void testGetFailure() throws Exception {
        doReturn(restHighLevelClient).when(elasticsearchConnection)
                .getClient();
        doThrow(new RuntimeException()).when(restHighLevelClient)
                .search(any(SearchRequest.class), any(RequestOptions.class));
        try {
            FqlGetRequest fqlGetRequest = new FqlGetRequest();
            fqlGetRequest.setTitle("title1");
            fqlGetRequest.setUserId("userId");
            fqlStoreServiceImpl.get(fqlGetRequest);
        } catch (FqlPersistenceException e) {
            assertEquals(ErrorCode.FQL_PERSISTENCE_EXCEPTION, e.getCode());
        }
    }

    @Test
    public void testGetWithoutUserId() {
        FqlGetRequest fqlGetRequest = new FqlGetRequest();
        fqlGetRequest.setTitle("title1");
        List<FqlStore> result = fqlStoreServiceImpl.get(fqlGetRequest);
        assertEquals(0, result.size());
    }

    @SneakyThrows
    @Test
    public void testGetWithoutTitle() {
        FqlGetRequest fqlGetRequest = new FqlGetRequest();
        fqlGetRequest.setUserId("userId");
        fqlGetRequest.setTitle("");
        SearchResponse searchResponse = mock(SearchResponse.class);

        doReturn(restHighLevelClient).when(elasticsearchConnection)
                .getClient();
        doReturn(searchResponse).when(restHighLevelClient)
                .search(new SearchRequest(FQL_STORE_INDEX).types(DOCUMENT_TYPE_NAME)
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(new SearchSourceBuilder().query(QueryBuilders.prefixQuery(USER_ID,
                                fqlGetRequest.getUserId()
                                        .toLowerCase()))
                                .from(fqlGetRequest.getFrom())
                                .size(fqlGetRequest.getSize())), RequestOptions.DEFAULT);
        doReturn(mock(SearchHits.class)).when(searchResponse)
                .getHits();
        List<FqlStore> result = fqlStoreServiceImpl.get(fqlGetRequest);
        assertEquals(0, result.size());

    }

    @SneakyThrows
    @Test
    public void testGetWithTitle() {
        FqlGetRequest fqlGetRequest = new FqlGetRequest();
        fqlGetRequest.setUserId("userId");
        fqlGetRequest.setTitle("title1");

        FqlStore fqlStore = new FqlStore();
        fqlStore.setTitle("title1");
        fqlStore.setUserId("userId");
        fqlStore.setId("Id");
        fqlStore.setQuery("query");

        String fqlStoreString = "{\"id\":\"Id\",\"userId\":\"userId\",\"title\":\"title1\",\"query\":\"query\"}";

        SearchHit[] hits = new SearchHit[]{searchHit};

        doReturn(restHighLevelClient).when(elasticsearchConnection)
                .getClient();
        doReturn(searchResponse).when(restHighLevelClient)
                .search(new SearchRequest(FQL_STORE_INDEX).types(DOCUMENT_TYPE_NAME)
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(new SearchSourceBuilder().query(QueryBuilders.prefixQuery(TITLE_FIELD,
                                fqlGetRequest.getTitle()
                                        .toLowerCase()))
                                .from(fqlGetRequest.getFrom())
                                .size(fqlGetRequest.getSize())), RequestOptions.DEFAULT);
        doReturn(searchHits).when(searchResponse)
                .getHits();
        doReturn(hits).when(searchHits)
                .getHits();
        doReturn(fqlStoreString).when(searchHit)
                .getSourceAsString();
        List<FqlStore> result = fqlStoreServiceImpl.get(fqlGetRequest);
        assertEquals(1, result.size());
        assertEquals(fqlStore, result.get(0));

    }
}

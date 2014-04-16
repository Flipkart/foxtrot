package com.flipkart.foxtrot.core.querystore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.MockHTable;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.actions.spi.ActionMetadata;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 16/04/14.
 */
public class QueryExecutorTest {
    QueryExecutor queryExecutor;
    QueryStore queryStore;
    ObjectMapper mapper = new ObjectMapper();
    MockElasticsearchServer elasticsearchServer = new MockElasticsearchServer();
    String TEST_APP = "test-app";

    @Before
    public void setUp() throws Exception {

        ElasticsearchUtils.setMapper(mapper);
        DataStore dataStore = getDataStore();

        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        PutIndexTemplateRequest templateRequest = ElasticsearchUtils.getClusterTemplateMapping(elasticsearchConnection.getClient().admin().indices());
        elasticsearchConnection.getClient().admin().indices().putTemplate(templateRequest).actionGet();
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        registerActions(analyticsLoader);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        this.queryExecutor = new QueryExecutor(analyticsLoader, executorService);

        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TEST_APP)).thenReturn(true);

        this.queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, queryExecutor);
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
    }

    @Test
    public void testExecute() throws Exception {
        testFilterAction();
        testGroupAction();
        testHistogramAction();
        testTrendAction();
    }

    private void testFilterAction() {

    }

    private void testGroupAction() throws QueryStoreException, JsonProcessingException {
        List<Document> testDocuments = getTestDocuments();
        queryStore.save(TEST_APP, testDocuments);
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TEST_APP);
        groupRequest.setFilters(new ArrayList<Filter>());
        groupRequest.setNesting(Arrays.asList("os", "version"));

        String expectedResult = "{\"opcode\":\"group\",\"result\":{\"android\":{\"1\":2,\"2\":1},\"iphone\":{\"1\":1,\"2\":1}}}";
        String actualResult = mapper.writeValueAsString(queryExecutor.execute(groupRequest));
        assertEquals(expectedResult, actualResult);
    }

    private void testHistogramAction() {

    }

    private void testTrendAction(){

    }

    @Test
    public void testExecuteAsync() throws Exception {

    }

    @Test
    public void testResolve() throws Exception {

    }

    private DataStore getDataStore() throws DataStoreException {
        HTableInterface tableInterface = MockHTable.create();
        HbaseTableConnection tableConnection = Mockito.mock(HbaseTableConnection.class);
        when(tableConnection.getTable()).thenReturn(tableInterface);
        return new HbaseDataStore(tableConnection, new ObjectMapper());
    }

    private List<Document> getTestDocuments(){
        List<Document> documents = new Vector<Document>();
        documents.add(getDocument(UUID.randomUUID().toString(), System.currentTimeMillis(), new Object[]{ "os", "android", "version", 1, "device", "nexus"}));
        documents.add(getDocument(UUID.randomUUID().toString(), System.currentTimeMillis(), new Object[]{ "os", "android", "version", 1, "device", "galaxy"}));
        documents.add(getDocument(UUID.randomUUID().toString(), System.currentTimeMillis(), new Object[]{ "os", "android", "version", 2, "device", "nexus"}));
        documents.add(getDocument(UUID.randomUUID().toString(), System.currentTimeMillis(), new Object[]{ "os", "iphone", "version", 1, "device", "iphone"}));
        documents.add(getDocument(UUID.randomUUID().toString(), System.currentTimeMillis(), new Object[]{ "os", "iphone", "version", 2, "device", "ipad"}));
        return documents;
    }

    private Document getDocument(String id, long timestamp, Object[] args){
        Map<String, Object> data = new HashMap<String, Object>();
        for ( int i = 0; i < args.length; i+= 2){
            data.put((String) args[i], args[i+1]);
        }
        return new Document(id, mapper.valueToTree(data));
    }

    private void registerActions(AnalyticsLoader analyticsLoader) throws Exception {
        Reflections reflections = new Reflections("com.flipkart.foxtrot", new SubTypesScanner());
        Set<Class<? extends Action>> actions = reflections.getSubTypesOf(Action.class);
        for(Class<? extends Action> action : actions) {
            AnalyticsProvider analyticsProvider = action.getAnnotation(AnalyticsProvider.class);
            if(null == analyticsProvider
                    || null == analyticsProvider.request()
                    || null == analyticsProvider.opcode()
                    || null == analyticsProvider.opcode()
                    || analyticsProvider.opcode().isEmpty()
                    || null == analyticsProvider.response()) {
//                throw new Exception("Invalid annotation on " + action.getCanonicalName());
            } else {
            analyticsLoader.register(new ActionMetadata(
                    analyticsProvider.request(), action,
                    false, analyticsProvider.opcode()));
            }
        }
    }



}

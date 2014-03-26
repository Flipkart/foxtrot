package com.flipkart.foxtrot.core.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.actions.spi.ActionMetadata;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Executors;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 3:59 PM
 */
public class ActionTest {
    private static class TestRequest implements ActionRequest {
        private String value;

        private TestRequest(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    private static class TestResponse implements ActionResponse {
        private String value;

        public TestResponse(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
    @VisibleForTesting
    public static class TestAction extends Action<TestRequest> {
        public TestAction(TestRequest parameter, DataStore dataStore, ElasticsearchConnection connection, String cacheToken) {
            super(parameter, dataStore, connection, cacheToken);
        }

        @Override
        protected String getRequestCacheKey() {
            return getParameter().getValue();
        }

        @Override
        public TestResponse execute(TestRequest parameter) throws QueryStoreException {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return new TestResponse(parameter.getValue());
        }

    }

    private static final class TestCache implements Cache {
        private Map<String, ActionResponse> data = Maps.newHashMap();

        @Override
        public ActionResponse put(String key, ActionResponse data) {
            this.data.put(key, data);
            return data;
        }

        @Override
        public ActionResponse get(String key) {
            if(data.containsKey(key)) {
                return data.get(key);
            }
            return null;
        }

        @Override
        public boolean has(String key) {
            return data.containsKey(key);
        }
    }

    private static final class TestCacheFactory implements CacheFactory {

        @Override
        public Cache create(String name) {
            return new TestCache();
        }
    }


    @Test
    public void testCacheKey() throws Exception {
/*
        Assert.assertEquals(UUID.nameUUIDFromBytes("HelloWorld".getBytes()).toString(),
                            new TestRequest("HelloWorld").getCachekey());
*/
    }

    @Test
    public void testExecute() throws Exception {
        /*QueryExecutor queryExecutor = new QueryExecutor(Executors.newFixedThreadPool(1), );
        CacheUtils.setCacheFactory(new TestCacheFactory());
        for(int i =0; i < 10; i++) {
            TestAction testAction = new TestAction();
            TestResponse response = queryExecutor.execute(testAction);
            Assert.assertEquals("Hello World", response.getValue());
            //System.out.println(new ObjectMapper().writeValueAsString(response));
        }
        TestAction testAction = new TestAction(new TestRequest("Hello Async World"));
        AsyncDataToken token = queryExecutor.executeAsync(testAction);
        while(null == testAction.get(token.getKey())) {
            Thread.sleep(50);
            //System.out.println("Waiting for " + key);
        }
        Assert.assertEquals("Hello Async World", testAction.get(token.getKey()).getValue());
        token = queryExecutor.executeAsync(testAction);
        while(null == testAction.get(token.getKey())) {
            Thread.sleep(50);
        }
        Assert.assertEquals("Hello Async World", testAction.get(token.getKey()).getValue());*/
    }

    @Test
    public void testExecute1() throws Exception {
        CacheUtils.setCacheFactory(new TestCacheFactory());
        ActionMetadata actionMetadata = new ActionMetadata(TestRequest.class, TestAction.class, true, "test");
        AnalyticsLoader loader = new AnalyticsLoader(null, null);
        loader.register(actionMetadata);
        QueryExecutor queryExecutor = new QueryExecutor(loader, Executors.newFixedThreadPool(1));
        ActionResponse response = queryExecutor.execute(new TestRequest("Hello"));
        System.out.println(new ObjectMapper().writeValueAsString(response));
    }

    @Test
    public void testGet() throws Exception {

    }

    @Test
    public void testCall() throws Exception {

    }

    @Test
    public void testExecute2() throws Exception {

    }

    @Test
    public void testIsCachable() throws Exception {

    }

    @Test
    public void testExecute3() throws Exception {

    }
}

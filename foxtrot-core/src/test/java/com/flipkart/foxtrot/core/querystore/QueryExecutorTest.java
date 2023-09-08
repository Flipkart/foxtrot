/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.common.RequestWithNoAction;
import com.flipkart.foxtrot.core.common.noncacheable.NonCacheableAction;
import com.flipkart.foxtrot.core.common.noncacheable.NonCacheableActionRequest;
import com.flipkart.foxtrot.core.config.OpensearchTuningConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchUtils;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.OpensearchTestUtils;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Created by rishabh.goyal on 02/05/14.
 */
public class QueryExecutorTest {

    private static HazelcastInstance hazelcastInstance;
    private static OpensearchConnection opensearchConnection;
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
    private AnalyticsLoader analyticsLoader;

    @BeforeClass
    public static void setupClass() throws Exception {
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        opensearchConnection = OpensearchTestUtils.getConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hazelcastInstance.shutdown();
        opensearchConnection.stop();
    }

    @Before
    public void setUp() throws Exception {
        DataStore dataStore = TestUtils.getDataStore();

        //Initializing Cache Factory
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(new Config());
        CacheManager cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));
        OpensearchUtils.initializeMappings(opensearchConnection.getClient());
        TableMetadataManager tableMetadataManager = mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(anyString())).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);
        QueryStore queryStore = mock(QueryStore.class);
        analyticsLoader = spy(
                new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, opensearchConnection, cacheManager,
                        mapper, new OpensearchTuningConfig()));
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService, Collections.emptyList());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testResolve() throws Exception {
        assertEquals(NonCacheableAction.class, queryExecutor.resolve(new NonCacheableActionRequest())
                .getClass());
    }

    @Test
    public void testResolveNonExistentAction() throws Exception {
        try {
            queryExecutor.resolve(new RequestWithNoAction());
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.UNRESOLVABLE_OPERATION, e.getCode());
        }
    }
}

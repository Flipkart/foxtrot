/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCache;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class DistributedCacheTest {
    private DistributedCache distributedCache;
    private HazelcastInstance hazelcastInstance;
    private ObjectMapper mapper;

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        mapper = spy(mapper);
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        distributedCache = new DistributedCache(hazelcastConnection, "TEST", mapper);
        CacheManager cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, mapper));

        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE_NAME)).thenReturn(true);
        QueryStore queryStore = Mockito.mock(QueryStore.class);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, null, queryStore, null, cacheManager, mapper);
        TestUtils.registerActions(analyticsLoader, mapper);
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstance.shutdown();
    }

    @Test
    public void testPut() throws Exception {
        ActionResponse expectedResponse = new GroupResponse(Collections.<String, Object>singletonMap("Hello", "world"));
        ActionResponse returnResponse = distributedCache.put("DUMMY_KEY_PUT", expectedResponse);
        assertEquals(expectedResponse, returnResponse);

        GroupResponse actualResponse = GroupResponse.class.cast(distributedCache.get("DUMMY_KEY_PUT"));
        assertEquals(GroupResponse.class.cast(expectedResponse).getResult(), actualResponse.getResult());
    }

    @Test
    public void testPutCacheException() throws Exception {
        doThrow(new JsonGenerationException("TEST_EXCEPTION")).when(mapper).writeValueAsString(any());
        ActionResponse returnResponse = distributedCache.put("DUMMY_KEY_PUT", null);
        verify(mapper, times(1)).writeValueAsString(any());
        assertNull(returnResponse);
        assertNull(hazelcastInstance.getMap("TEST").get("DUMMY_KEY_PUT"));
    }

    @Test
    public void testGet() throws Exception {
        GroupResponse baseRequest = new GroupResponse();
        baseRequest.setResult(Collections.<String, Object>singletonMap("Hello", "World"));
        String requestString = mapper.writeValueAsString(baseRequest);
        distributedCache.put("DUMMY_KEY_GET", mapper.readValue(requestString, ActionResponse.class));
        ActionResponse actionResponse = distributedCache.get("DUMMY_KEY_GET");
        String actualResponse = mapper.writeValueAsString(actionResponse);
        assertEquals(requestString, actualResponse);
    }

    @Test
    public void testGetInvalidKeyValue() throws Exception {
        assertNull(distributedCache.get("DUMMY_KEY_GET"));
    }

    @Test
    public void testGetNullKey() throws Exception {
        assertNull(distributedCache.get(null));
    }

    @Test
    public void testHas() throws Exception {
        GroupResponse baseRequest = new GroupResponse();
        baseRequest.setResult(Collections.<String, Object>singletonMap("Hello", "World"));
        distributedCache.put("DUMMY_KEY_HAS", baseRequest);
        boolean response = distributedCache.has("DUMMY_KEY_HAS");
        assertTrue(response);
        response = distributedCache.has("INVALID_KEY");
        assertFalse(response);
        response = distributedCache.has(null);
        assertFalse(response);
    }
}

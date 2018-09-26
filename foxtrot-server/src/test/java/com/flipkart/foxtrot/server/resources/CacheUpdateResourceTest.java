package com.flipkart.foxtrot.server.resources;/**
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

import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/***
 Created by nitish.goyal on 17/08/18
 ***/
public class CacheUpdateResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources;

    public CacheUpdateResourceTest() throws Exception {
        super();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        resources = ResourceTestRule.builder()
                .addResource(new CacheUpdateResource(executorService, getTableMetadataManager()))
                .addProvider(new FoxtrotExceptionMapper(getMapper()))
                .setMapper(getMapper())
                .build();
    }

    @Test
    public void testUpdateCache() {
        resources.client().target("/v1/cache/update/cardinality").request().post(null);

    }
}

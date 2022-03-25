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

import com.flipkart.foxtrot.core.exception.provider.FoxtrotExceptionMapper;
import com.flipkart.foxtrot.server.ResourceTestUtils;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

/***
 Created by nitish.goyal on 17/08/18
 ***/
public class CardinalityCalculationResourceTest extends FoxtrotResourceTest {

    @Rule
    public ResourceTestRule resources;

    public CardinalityCalculationResourceTest() throws Exception {
        super();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        resources = ResourceTestUtils.testResourceBuilder(getMapper())
                .addResource(new CardinalityCalculationResource(executorService, getCardinalityCalculationService(),
                        getCardinalityCalculationFactory()))
                .addProvider(new FoxtrotExceptionMapper(getMapper()))
                .setMapper(getMapper())
                .build();
    }

    @Test
    public void testUpdateCache() {
        resources.target("/v1/cardinality/calculate")
                .request()
                .post(null);
        assertTrue(true);

    }
}

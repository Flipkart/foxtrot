package impl;/**
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

/***
 Created by nitish.goyal on 02/08/18
 ***/

import com.flipkart.foxtrot.core.querystore.impl.OpensearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import org.opensearch.client.Client;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/***
 Created by nitish.goyal on 01/08/18
 ***/
@Category(IntegrationTest.class)
public class FieldMetadataStoreTest {

    @Before
    public void setup() throws Exception {
        // create an index, make sure it is ready for indexing and add documents to it
        //createIndex("test");
        //ensureGreen("test");

    }

    @Test
    public void testStore() throws Exception {

        //ObjectMapper mapper = new ObjectMapper();
        //mapper.registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES));

        OpensearchConfig config = new OpensearchConfig();
        config.setCluster("test");
        config.setHosts("localhost");
        config.setTableNamePrefix("foxtrot");

        OpensearchConnection connection = new OpensearchConnection(config);
        connection.start();
        //CachedFieldMetadataStore fieldMetadataStore
        //= new CachedFieldMetadataStore(new ESFieldMetadataStore(mapper, connection));
        Client client = connection.getClient();

        System.out.println("Client : " + client.toString());
        // Test code ....

    }

}
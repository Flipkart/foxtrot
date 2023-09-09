package com.flipkart.foxtrot.core.table.impl;
/*
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
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

import com.flipkart.foxtrot.core.querystore.impl.OpensearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import java.util.Collections;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.testcontainers.OpensearchContainer;

/***
 Created by nitish.goyal on 02/08/18
 ***/
@Slf4j
public class OpensearchTestUtils {

    public static synchronized OpensearchConnection getConnection() throws Exception {
        // To make sure we load class which will start the server.
        OpensearchContainerHolder.containerLoaded = true;
        OpensearchConnection opensearchConnection = new OpensearchConnection(
                OpensearchContainerHolder.getOpensearchConfig());
        opensearchConnection.start();

        return opensearchConnection;
    }

    public static void cleanupIndices(final OpensearchConnection opensearchConnection) {
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("_all");
            final AcknowledgedResponse deleteIndexResponse = opensearchConnection.getClient()
                    .indices()
                    .delete(deleteIndexRequest, RequestOptions.DEFAULT);
            log.info("Delete index response: {}", deleteIndexResponse);
        } catch (Exception e) {
            log.error("Index Cleanup failed", e);
        }
    }

    /**
     * Class to make sure we run the server only once.
     */
    private static class OpensearchContainerHolder {

        @SuppressWarnings("unused")
        private static boolean containerLoaded;

        @Getter
        private static OpensearchConfig opensearchConfig;

        static {
            try {
                OpensearchContainer opensearchContainer = new OpensearchContainer("opensearchproject/opensearch:2.0.0");
                opensearchContainer.start();

                Integer mappedPort = opensearchContainer.getMappedPort(9200);

                opensearchConfig = new OpensearchConfig();
                opensearchConfig.setHosts(Collections.singletonList(opensearchContainer.getHttpHostAddress()));
                opensearchConfig.setPort(mappedPort);
                opensearchConfig.setConnectionType(OpensearchConfig.ConnectionType.HTTP);
                opensearchConfig.setCluster("opensearch");
                opensearchConfig.setTableNamePrefix("foxtrot");
            } catch (Exception e) {
                log.error("Error in initializing es test container , error :", e);
                throw e;
            }
        }
    }
}

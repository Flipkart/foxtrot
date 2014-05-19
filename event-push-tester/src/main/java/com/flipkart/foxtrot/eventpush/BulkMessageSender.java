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
package com.flipkart.foxtrot.eventpush;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;

import javax.ws.rs.core.MediaType;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;

/**
 * Created by rishabh.goyal on 08/05/14.
 */
public class BulkMessageSender implements Callable<Void> {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(BulkMessageSender.class.getSimpleName());
    private int numBatches;
    private int bulkSize;
    private ObjectMapper objectMapper;
    private int threadID;
    private String hostName;
    private CloseableHttpClient httpClient;
    private HttpClientContext httpContext;
    private String targetHost;
    private int targetPort;

    public BulkMessageSender(int numBatches,
                             int bulkSize,
                             ObjectMapper objectMapper,
                             int threadID,
                             String hostName,
                             CloseableHttpClient httpClient, String targetHost, int targetPort) {
        this.numBatches = numBatches;
        this.bulkSize = bulkSize;
        this.objectMapper = objectMapper;
        this.threadID = threadID;
        this.hostName = hostName;
        this.httpClient = httpClient;
        this.httpContext = HttpClientContext.create();
        this.targetHost = targetHost;
        this.targetPort = targetPort;
    }


    @Override
    public Void call() throws Exception {
        for (int i = 1; i <= numBatches; i++) {
            List<Document> documents = new Vector<Document>();
            for (int j = 1; j <= bulkSize; j++) {
                Document document = new Document();
                ObjectNode rootNode = objectMapper.createObjectNode();
                rootNode.put("value", "Santanu Sinha");
                rootNode.put("threadID", threadID);
                rootNode.put("hostName", hostName);
                rootNode.put("count", i);
                rootNode.put("timestamp", System.currentTimeMillis());
                document.setId(UUID.randomUUID().toString());
                document.setTimestamp(System.currentTimeMillis());
                document.setData(rootNode);
                documents.add(document);
            }
            HttpPost httpPost = new HttpPost(String.format("http://%s:%d/foxtrot/v1/document/test/bulk", targetHost, targetPort));
            httpPost.setHeader(new BasicHeader("Content-Type", MediaType.APPLICATION_JSON));
            httpPost.setEntity(new StringEntity(objectMapper.writeValueAsString(documents), Charset.defaultCharset()));
            CloseableHttpResponse entity = httpClient.execute(httpPost, httpContext);
            try {
                if (entity.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    logger.info(EntityUtils.toString(entity.getEntity()));
                } else {
                    logger.error(entity.getStatusLine().getStatusCode() + ":" + entity.getStatusLine().getReasonPhrase());
                }

            } finally {
                entity.close();
            }
        }
        return null;
    }
}
